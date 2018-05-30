package com.poc.sample


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.poc.sample.Models._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, max, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.Breaks._

object LoadDataToHive {

  val logger = LoggerFactory.getLogger(LoadDataToHive.getClass)

  def reconcile(materialConfig: MaterialConfig, partitionColumnList: Seq[String], uniqueKeyList: Seq[String], mandatoryMetaData: Seq[String], hiveContext: HiveContext): CIANotification = {

    hiveContext.sql(s"use ${materialConfig.hiveDatabase}")
    val incrementalDataframe = hiveContext.table(materialConfig.incrementalTableName)
    val incrementalUBFreeDataframe = incrementalDataframe.filter(incrementalDataframe(materialConfig.headerOperation).notEqual("UB"))

    val partitionColumns = partitionColumnList.mkString(",")
    val ciaNotification: CIANotification = materialConfig.versionIndicator match {
      case "Y" =>
        val currentTimestamp = materializeAndKeepVersion(materialConfig.baseTableName, hiveContext, incrementalUBFreeDataframe, partitionColumnList, partitionColumns)
        val notification: CIANotification = buildNotificationObject(materialConfig.pathToLoad, materialConfig.hiveDatabase, materialConfig.baseTableName, materialConfig.seqColumn, incrementalUBFreeDataframe, currentTimestamp)
        notification
      case "N" =>
        val currentTimestamp = materializeWithLatestVersion(materialConfig.hiveDatabase, materialConfig.baseTableName, materialConfig.incrementalTableName, uniqueKeyList, partitionColumnList, materialConfig.seqColumn, hiveContext, incrementalDataframe, partitionColumns, materialConfig.headerOperation, materialConfig.deleteIndicator, mandatoryMetaData, materialConfig)
        val notification: CIANotification = buildNotificationObject(materialConfig.pathToLoad, materialConfig.hiveDatabase, materialConfig.baseTableName, materialConfig.seqColumn, incrementalUBFreeDataframe, currentTimestamp)
        notification
    }

    ciaNotification

  }


  def buildNotificationObject(pathToLoad: String, hiveDatabase: String, baseTableName: String, seqColumn: String, incrementalDataframe: DataFrame, currentTimestamp: String) = {
    val latestTimeStamp = findLatestTSInRecords(seqColumn, incrementalDataframe)
    val notification = CIANotification(hiveDatabase, baseTableName, pathToLoad, latestTimeStamp, currentTimestamp)
    notification
  }

  def findLatestTSInRecords(seqColumn: String, incrementalDataframe: DataFrame): String = {
    val maxTimestamp = incrementalDataframe.agg(max(seqColumn))
    val latestTimeStamp = maxTimestamp.collect().map(_.getString(0)).mkString(" ")
    latestTimeStamp
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def materializeWithLatestVersion(hiveDatabase: String, baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, hiveContext: HiveContext, incrementalDataframe: DataFrame, partitionColumns: String, headerOperation: String, deleteIndicator: String, mandatoryMetaData: Seq[String], materialConfig: MaterialConfig): String = {

    val baseDataFrame = if (partitionColumns.isEmpty) {
      val baseTableDataframe = hiveContext.table(baseTableName)
      val fieldList = scala.collection.mutable.MutableList[AdditionalFields]()
      var alterIndicator = false
      breakable {
        mandatoryMetaData.foreach(metadata => {
          if (!hasColumn(baseTableDataframe, metadata)) {
            val typeWithNullable = Array("string", "null")
            val fields = AdditionalFields(metadata, metadata, typeWithNullable)
            fieldList += fields
            alterIndicator = true
          }
          else {
            break()
          }
        })
      }

      if (alterIndicator) {
        val avroSchemaString = buildAvroSchema(hiveDatabase, baseTableDataframe.schema, baseTableName, fieldList.toArray)
        val incrementalExtTable =
          s"""
             |ALTER table $baseTableName \n
             |SET TBLPROPERTIES('avro.schema.literal' = '$avroSchemaString')
                     """.stripMargin
        hiveContext.sql(incrementalExtTable)
      }

      val baseTableData = hiveContext.table(baseTableName)
      baseTableData
    } else {
      val partitionWhereClause: String = getIncrementPartitions(incrementalTableName, partitionColumnList, hiveContext, partitionColumns)
      val basePartitionsDataframe: DataFrame = getBaseTableDataFromIncPartitions(baseTableName, hiveContext, partitionColumns, partitionWhereClause)
      basePartitionsDataframe
    }

    val upsertDataframe: DataFrame = uniqueKeyList.flatten.length match {
      case 0 => getUpsertBaseTableDataNoUniqueKeys(hiveContext, baseDataFrame, incrementalDataframe, uniqueKeyList, seqColumn, headerOperation, deleteIndicator, mandatoryMetaData, materialConfig)
      case _ => getUpsertBaseTableData(hiveContext, baseDataFrame, incrementalDataframe, uniqueKeyList, seqColumn, headerOperation, deleteIndicator)
    }
    logger.warn(s"Upserted data have been found for the table ${baseTableName} and the hive tables would be loaded now")
    val currentTimestamp = if (partitionColumns.isEmpty) {
      writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName, "overwrite", upsertDataframe)
    } else {
      writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "overwrite", upsertDataframe)
    }
    currentTimestamp
  }


  def buildAvroSchema(hiveDatabase: String, rawSchema: StructType, baseTableName: String, mandatoryMetadataArray: Array[AdditionalFields]) = {
    val schemaList = rawSchema.fields.map(field => AdditionalFields(field.name, field.name, Array(field.dataType.typeName, "null")))
    val finalSchemaList = schemaList ++ mandatoryMetadataArray
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val avroSchema = BaseAvroSchema("record", baseTableName, hiveDatabase, finalSchemaList)
    val avroSchemaString = mapper.writeValueAsString(avroSchema)
    avroSchemaString
  }

  def materializeAndKeepVersion(baseTableName: String, hiveContext: HiveContext, incrementalDataframe: DataFrame, partitionColumnList: Seq[String], partitionColumns: String): String = {
    val currentTimestamp = partitionColumnList match {
      case Nil => writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName, "append", incrementalDataframe)
      case _ => writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "append", incrementalDataframe)
    }
    currentTimestamp
  }

  def writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName: String, writeMode: String, upsertDataframe: DataFrame): String = {
    upsertDataframe
      .write
      .format("com.databricks.spark.avro")
      .mode(writeMode)
      .insertInto(baseTableName)
    logger.warn(s"Materialized data have been written on the hive table ${baseTableName}")
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
  }

  def writeUpsertDataBackToBasePartitions(baseTableName: String, partitionColumns: String, writeMode: String, upsertDataframe: DataFrame): String = {
    upsertDataframe
      .write
      .format("com.databricks.spark.avro")
      .mode(writeMode)
      .partitionBy(partitionColumns)
      .insertInto(baseTableName)
    logger.warn(s"Materialized data have been written on the hive table ${baseTableName}")
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
  }

  def getBaseTableDataFromIncPartitions(baseTableName: String, hiveContext: HiveContext, partitionColumns: String, partitionWhereClause: String) = {
    val baseTablePartitionQuery =
      s"""
         |Select * from $baseTableName where $partitionColumns in ($partitionWhereClause)  \n
       """.stripMargin
    val baseTableDataframe = hiveContext.sql(baseTablePartitionQuery)
    baseTableDataframe
  }

  def getIncrementPartitions(incrementalTableName: String, partitionColumnList: Seq[String], hiveContext: HiveContext, partitionColumns: String) = {
    val incTableParColQuery =
      s"""
         |Select $partitionColumns from $incrementalTableName \n
       """.stripMargin
    val incTableParColDF = hiveContext.sql(incTableParColQuery)
    val noDuplDF = incTableParColDF.dropDuplicates(partitionColumnList)
    val noDuplList = noDuplDF.select(partitionColumns).map(row => row(0).asInstanceOf[String]).collect()
    val partitionWhereClause = noDuplList.mkString(",")
    partitionWhereClause
  }


  def getUpsertBaseTableDataNoUniqueKeys(hiveContext: HiveContext, baseTableDataframe: DataFrame, incrementalData: DataFrame, uniqueKeyList: Seq[String], seqColumn: String, headerOperation: String, deleteIndicator: String, mandatoryMetaData: Seq[String], materialConfig: MaterialConfig): DataFrame = {
    val duplicateFreeIncrementDF = incrementalData.dropDuplicates()
    val tsAppendedIncDF = duplicateFreeIncrementDF.withColumn("modified_timestamp", lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)))
    val beforeImageDF = tsAppendedIncDF.filter(tsAppendedIncDF(headerOperation).equalTo("UB"))
    val deleteImageDF = tsAppendedIncDF.filter(tsAppendedIncDF(headerOperation).equalTo(deleteIndicator))
    val baseData = mandatoryMetaData.foldLeft(baseTableDataframe) {
      (acc: DataFrame, colName: String) =>
        acc.drop(colName)
    }
    val baseTableColumns = baseData.columns
    val duplicateFreeBaseData = baseData
      .except(beforeImageDF.select(baseTableColumns.head, baseTableColumns.tail: _*))
      .except(deleteImageDF.select(baseTableColumns.head, baseTableColumns.tail: _*))
    val lowercaseMandata = mandatoryMetaData.map(_.toLowerCase)
    val baseDataframeColumns = baseTableDataframe.columns.filterNot(lowercaseMandata.toSet)
    val resultDFjoined = baseTableDataframe.join(duplicateFreeBaseData, baseDataframeColumns)
    val deleteAndBeforeFreeIncrement = tsAppendedIncDF.filter(tsAppendedIncDF(headerOperation).notEqual(deleteIndicator))
      .filter(tsAppendedIncDF(headerOperation).notEqual("UB"))
    val resultDF = resultDFjoined.unionAll(deleteAndBeforeFreeIncrement)
    val cleanedUpDF = resultDF.filter(resultDF(headerOperation).notEqual(deleteIndicator))
      .filter(resultDF(headerOperation).notEqual("UB"))
    val windowFunction = Window.partitionBy(baseTableColumns.head, baseTableColumns.tail: _*).orderBy(desc(seqColumn))
    val duplicateFreeCleanedUpDF = cleanedUpDF.withColumn("rownum", row_number.over(windowFunction)).where("rownum = 1").drop("rownum")
    duplicateFreeCleanedUpDF
  }


  def getUpsertBaseTableData(hiveContext: HiveContext, baseTableDataframe: DataFrame, incrementalData: DataFrame, uniqueKeyList: Seq[String], seqColumn: String, headerOperation: String, deleteIndicator: String): DataFrame = {
    val incrementalDataFrame = incrementalData.filter(incrementalData(headerOperation).notEqual("UB"))
    val windowFunction = Window.partitionBy(uniqueKeyList.head, uniqueKeyList.tail: _*).orderBy(desc(seqColumn))
    val duplicateFreeIncrementDF = incrementalDataFrame.withColumn("rownum", row_number.over(windowFunction)).where("rownum = 1").drop("rownum")
    val tsAppendedIncDF = duplicateFreeIncrementDF.withColumn("modified_timestamp", lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)))
    val columns = baseTableDataframe.columns
    val incrementDataFrame = tsAppendedIncDF.toDF(tsAppendedIncDF.columns.map(x => x.trim + "_i"): _*)
    val joinExprs = uniqueKeyList
      .zip(uniqueKeyList)
      .map { case (c1, c2) => baseTableDataframe(c1) === incrementDataFrame(c2 + "_i") }
      .reduce(_ && _)
    val joinedDataFrame = baseTableDataframe.join(incrementDataFrame, joinExprs, "outer")
    val upsertDataFrame = columns.foldLeft(joinedDataFrame) {
      (acc: DataFrame, colName: String) =>
        acc.withColumn(colName + "_j", hasColumn(joinedDataFrame, colName + "_i") match {
          case true => coalesce(col(colName + "_i"), col(colName))
          case false => col(colName)
        })
          .drop(colName)
          .drop(colName + "_i")
          .withColumnRenamed(colName + "_j", colName)
    }
    val upsertedColumns = upsertDataFrame.columns
    val additionalColumns = upsertedColumns diff columns
    val materializedDataframe = additionalColumns.foldLeft(upsertDataFrame) {
      (acc: DataFrame, colName: String) =>
        acc.drop(colName)
    }
    val deleteUpsertFreeDataframe = materializedDataframe.filter(materializedDataframe(headerOperation).notEqual(deleteIndicator))
    deleteUpsertFreeDataframe
  }

}
