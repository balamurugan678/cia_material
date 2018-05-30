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

    keyTabRefresh(ciaMaterialConfig, hadoopConfig)

    ciaMaterialConfig.materialConfigs.par.foreach(materialConfig => {
      materializeTable(hadoopConfig, hadoopFileSystem, sparkContext, hiveContext, materialConfig)
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

  def materializeTable(hadoopConfig: Configuration, hadoopFileSystem: FileSystem, sparkContext: SparkContext, hiveContext: HiveContext, materialConfig: MaterialConfig) = {

    val uniqueKeyList = materialConfig.uniqueKeyList.split('|').toSeq
    val partitionColumns = materialConfig.partitionColumns.split('|').toSeq
    val mandatoryMetaData = materialConfig.mandatoryMetaData.split('|').toSeq

    logger.warn(s"Materialization started at ${LocalDateTime.now} for the table ${materialConfig.baseTableName} and the delta files would be picked from ${materialConfig.pathToLoad}")
    IncrementalTableSetUp.loadIncrementalData(materialConfig, hiveContext) match {
      case Success(success) => {
        val ciaNotification = LoadDataToHive.reconcile(materialConfig, partitionColumns, uniqueKeyList, mandatoryMetaData, hiveContext)
        MaterializationCloseDown.dropIncrementalExtTable(materialConfig, hiveContext)
        MaterializationCloseDown.moveFilesToProcessedDirectory(materialConfig, hadoopConfig, hadoopFileSystem)
        logger.warn(s"Materialization finished at ${LocalDateTime.now} for the table ${materialConfig.baseTableName} and the cleaned up happened!!!")
        //MaterializationNotification.persistNotificationInES(sparkContext, ciaNotification)
      }
      case Failure(ex) => {
        ex match {
          case fne: FileNotFoundException => logger.error(s"No delta avro files present for the table ${materialConfig.baseTableName} at the path ${materialConfig.pathToLoad}. Moving onto the next config!!")
        }
      }
    }
  }

  def parseMaterializationConfig(sparkContext: SparkContext) = {
    val sparkConfigJSONString = Source.fromFile(sparkContext.getConf.get("spark.configFileLocation")).mkString
    implicit val formats = DefaultFormats
    val json = parse(sparkConfigJSONString)
    val ciaMaterialConfig = json.extract[CIAMaterialConfig]
    ciaMaterialConfig
  }

}
