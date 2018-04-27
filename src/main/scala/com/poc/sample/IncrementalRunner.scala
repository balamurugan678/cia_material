package com.poc.sample

import com.poc.sample.Models.{CIAMaterialConfig, MaterialConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

object IncrementalRunner {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("IncrementalRunner")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val ciaMaterialConfig: CIAMaterialConfig = parseMaterializationConfig(sparkContext)

    val hadoopConfig = sparkContext.hadoopConfiguration
    val hadoopFileSystem = FileSystem.get(hadoopConfig)
    val hiveContext = new HiveContext(sparkContext)

    ciaMaterialConfig.materialConfigs.foreach(materialConfig => {
      materializeTable(hadoopConfig, hadoopFileSystem, sparkContext, hiveContext, materialConfig)
    })

  }

  def materializeTable(hadoopConfig: Configuration, hadoopFileSystem: FileSystem, sparkContext:SparkContext, hiveContext: HiveContext, materialConfig: MaterialConfig) = {
    val hiveDatabase = materialConfig.hiveDatabase
    val baseTableName = materialConfig.baseTableName
    val incrementalTableName = materialConfig.incrementalTableName
    val pathToLoad = materialConfig.pathToLoad
    val processedPathToMove = materialConfig.processedPathToMove
    val uniqueKeyList = materialConfig.uniqueKeyList.split('|').toSeq
    val partitionColumns = materialConfig.partitionColumns.split('|').toSeq
    val seqColumn = materialConfig.seqColumn
    val versionIndicator = materialConfig.versionIndicator

    IncrementalTableSetUp.loadIncrementalData(pathToLoad, hiveDatabase, incrementalTableName, hiveContext)

    val ciaNotification = LoadDataToHive.reconcile(pathToLoad, hiveDatabase, baseTableName, incrementalTableName, uniqueKeyList, partitionColumns, seqColumn, versionIndicator, hiveContext)

    MaterializationCloseDown.dropIncrementalExtTable(incrementalTableName, hiveContext)

    MaterializationCloseDown.moveFilesToProcessedDirectory(hadoopConfig, hadoopFileSystem, pathToLoad, processedPathToMove)

    MaterializationNotification.persistNotificationInES(sparkContext, ciaNotification)

  }

  def parseMaterializationConfig(sparkContext: SparkContext) = {
    val sparkConfigJSONString = Source.fromFile(sparkContext.getConf.get("spark.configFileLocation")).mkString
    implicit val formats = DefaultFormats
    val json = parse(sparkConfigJSONString)
    val ciaMaterialConfig = json.extract[CIAMaterialConfig]
    ciaMaterialConfig
  }

}
