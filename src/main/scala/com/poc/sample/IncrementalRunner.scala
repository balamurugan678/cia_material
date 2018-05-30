package com.poc.sample

import java.time.LocalDateTime

import com.poc.sample.Models.{CIAMaterialConfig, MaterialConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.io.Source

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


    ciaMaterialConfig.materialConfigs.par.foreach(materialConfig => {
      materializeTable(hadoopConfig, hadoopFileSystem, sparkContext, hiveContext, materialConfig)
    })


  }

  def materializeTable(hadoopConfig: Configuration, hadoopFileSystem: FileSystem, sparkContext: SparkContext, hiveContext: HiveContext, materialConfig: MaterialConfig) = {
    val hiveDatabase = materialConfig.hiveDatabase
    val baseTableName = materialConfig.baseTableName
    val incrementalTableName = materialConfig.incrementalTableName
    val pathToLoad = materialConfig.pathToLoad
    val processedPathToMove = materialConfig.processedPathToMove
    val uniqueKeyList = materialConfig.uniqueKeyList.split('|').toSeq
    val partitionColumns = materialConfig.partitionColumns.split('|').toSeq
    val seqColumn = materialConfig.seqColumn
    val versionIndicator = materialConfig.versionIndicator
    val headerOperation = materialConfig.headerOperation
    val deleteIndicator = materialConfig.deleteIndicator
    val mandatoryMetaData = materialConfig.mandatoryMetaData.split('|').toSeq

    logger.warn(s"Materialization started at ${LocalDateTime.now} for the table ${baseTableName} and the delta files would be picked from ${pathToLoad}")

    //try{
    IncrementalTableSetUp.loadIncrementalData(pathToLoad, hiveDatabase, baseTableName, incrementalTableName, hiveContext)

    val ciaNotification = LoadDataToHive.reconcile(pathToLoad, hiveDatabase, baseTableName, incrementalTableName, uniqueKeyList, partitionColumns, seqColumn, versionIndicator, headerOperation, deleteIndicator, mandatoryMetaData, hiveContext, materialConfig)

    MaterializationCloseDown.dropIncrementalExtTable(incrementalTableName, hiveContext)

    MaterializationCloseDown.moveFilesToProcessedDirectory(hadoopConfig, hadoopFileSystem, pathToLoad, processedPathToMove)

    logger.warn(s"Materialization finished at ${LocalDateTime.now} for the table ${baseTableName} and the cleaned up happened!!!")
    //MaterializationNotification.persistNotificationInES(sparkContext, ciaNotification)
    /*}
    catch {
      case ex:Exception => println(s"No delta avro files present for the table $baseTableName at the path $pathToLoad. Moving onto the next config!!")
    }
    finally {
      println(s"Materialization is done for the table $baseTableName with the change data at $pathToLoad. Moving onto the next config!!")
    }*/
  }

  def parseMaterializationConfig(sparkContext: SparkContext) = {
    val sparkConfigJSONString = Source.fromFile(sparkContext.getConf.get("spark.configFileLocation")).mkString
    implicit val formats = DefaultFormats
    val json = parse(sparkConfigJSONString)
    val ciaMaterialConfig = json.extract[CIAMaterialConfig]
    ciaMaterialConfig
  }

}
