package com.poc.sample

import java.io.FileNotFoundException
import java.time.LocalDateTime

import com.poc.sample.Models.{CIAMaterialConfig, MaterialConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success}

object IncrementalRunner {

  val logger = LoggerFactory.getLogger(IncrementalRunner.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("IncrementalRunner")
    val sparkContext = new SparkContext(sparkConf)
    val ciaMaterialConfig: CIAMaterialConfig = parseMaterializationConfig(sparkContext)
    val hadoopConfig = sparkContext.hadoopConfiguration
    val hadoopFileSystem = FileSystem.get(hadoopConfig)
    val hiveContext = new HiveContext(sparkContext)
    val journalControlFields = ciaMaterialConfig.cdcJournalControlFields

    keyTabRefresh(ciaMaterialConfig, hadoopConfig)

    ciaMaterialConfig.materialConfigs.foreach(materialConfig => {
      materializeTable(hadoopConfig, hadoopFileSystem, sparkContext, hiveContext, materialConfig, journalControlFields, ciaMaterialConfig.esStatusIndicator, ciaMaterialConfig)
    })

  }

  def keyTabRefresh(ciaMaterialConfig: CIAMaterialConfig, hadoopConfig: Configuration): Unit = {
    ciaMaterialConfig.environment match {
      case "local" => Unit
      case _ => {
        UserGroupInformation.setConfiguration(hadoopConfig)
        UserGroupInformation.loginUserFromKeytab(ciaMaterialConfig.kerberosPrincipal, ciaMaterialConfig.kerberosKeyTabLocation)
      }
    }

  }

  def materializeTable(hadoopConfig: Configuration, hadoopFileSystem: FileSystem, sparkContext: SparkContext, hiveContext: HiveContext,
                       materialConfig: MaterialConfig, journalControlFields: String, esStatusIndicator: Boolean, ciaMaterialConfig: CIAMaterialConfig) = {

    val matConfig: MaterialConfig = findAndOverrideProperties(materialConfig, ciaMaterialConfig)

    val uniqueKeyList = matConfig.uniqueKeyList.split('|').toSeq
    val partitionColumns = matConfig.partitionColumns.split('|').toSeq
    val mandatoryMetaData = matConfig.mandatoryMetaData.split('|').toSeq
    val controlFields: Seq[String] = journalControlFields.split('|').toSeq.map(_.toLowerCase)


    logger.warn(s"Materialization started at ${LocalDateTime.now} for the table ${matConfig.baseTableName} and the delta files would be picked from ${matConfig.pathToLoad}")
    val startTimeMills = System.currentTimeMillis()
    IncrementalTableSetUp.loadIncrementalData(hadoopFileSystem, hadoopConfig, matConfig, ciaMaterialConfig, hiveContext, controlFields) match {
      case Success(success) => {
        val ciaNotification = LoadDataToHive.reconcile(matConfig, ciaMaterialConfig, partitionColumns, uniqueKeyList, mandatoryMetaData, hiveContext)
        if (!materialConfig.incrementalHiveTableExist)
          MaterializationCloseDown.dropIncrementalExtTable(matConfig, hiveContext)
        MaterializationCloseDown.moveFilesToProcessedDirectory(matConfig, ciaMaterialConfig, hadoopConfig, hadoopFileSystem)
        logger.warn(s"Materialization finished at ${LocalDateTime.now} for the table ${matConfig.baseTableName} and the clean up happened!!!")
        val endTimeMills = System.currentTimeMillis()
        val durationSeconds = (endTimeMills - startTimeMills) / 1000
        val finalCIANotification = ciaNotification.copy(timeTaken = durationSeconds.toString)
        if (esStatusIndicator)
          MaterializationNotification.persistNotificationInES(sparkContext, finalCIANotification)
        logger.warn(s"Notification is: $finalCIANotification")
      }
      case Failure(ex) => {
        ex match {
          case fne: FileNotFoundException => logger.error(s"No delta avro files present for the table ${matConfig.baseTableName} at the path ${matConfig.pathToLoad}. Moving onto the next config!!")
        }
      }
    }
  }

  def findAndOverrideProperties(materialConfig: MaterialConfig, ciaMaterialConfig: CIAMaterialConfig) = {
    val matConfig = if (ciaMaterialConfig.overrideIndicator) {
      materialConfig.copy(createBaseTable = ciaMaterialConfig.createBaseTable, createBaseTableFromScooped = ciaMaterialConfig.createBaseTableFromScooped,
        incrementalHiveTableExist = ciaMaterialConfig.incrementalHiveTableExist, seqColumn = ciaMaterialConfig.seqColumn,
        headerOperation = ciaMaterialConfig.headerOperation, deleteIndicator = ciaMaterialConfig.deleteIndicator, beforeImageIndicator = ciaMaterialConfig.beforeImageIndicator,
        mandatoryMetaData = ciaMaterialConfig.mandatoryMetaData)
    }
    else {
      materialConfig
    }
    matConfig
  }

  def parseMaterializationConfig(sparkContext: SparkContext) = {
    val sparkConfigJSONString = Source.fromFile(sparkContext.getConf.get("spark.configFileLocation")).mkString
    implicit val formats = DefaultFormats
    val json = parse(sparkConfigJSONString)
    val ciaMaterialConfig = json.extract[CIAMaterialConfig]
    ciaMaterialConfig
  }


}
