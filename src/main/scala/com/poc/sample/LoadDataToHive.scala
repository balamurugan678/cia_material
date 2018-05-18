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

  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def reconcile(pathToLoad: String, hiveDatabase: String, baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, versionIndicator: String, headerOperation: String, deleteIndicator: String, mandatoryMetaData: Seq[String], hiveContext: HiveContext): CIANotification = {

    hiveContext.sql(s"use $hiveDatabase")

    val incrementalDataframe = hiveContext.table(incrementalTableName)
    println("******Incremental External Table******* with " + incrementalDataframe.count() + " rows")
    incrementalDataframe.show()

    val incrementalUBFreeDataframe = incrementalDataframe.filter(incrementalDataframe(headerOperation).notEqual("UB"))

    val partitionColumns = partitionColumnList.mkString(",")
    val ciaNotification: CIANotification = versionIndicator match {
      case "Y" =>
        val currentTimestamp = materializeAndKeepVersion(baseTableName, hiveContext, incrementalUBFreeDataframe, partitionColumnList, partitionColumns)
        val notification: CIANotification = buildNotificationObject(pathToLoad, hiveDatabase, baseTableName, seqColumn, incrementalUBFreeDataframe, currentTimestamp)
        notification
      case "N" =>
        val currentTimestamp = materializeWithLatestVersion(hiveDatabase, baseTableName, incrementalTableName, uniqueKeyList, partitionColumnList, seqColumn, hiveContext, incrementalUBFreeDataframe, partitionColumns, headerOperation, deleteIndicator, mandatoryMetaData)
        val notification: CIANotification = buildNotificationObject(pathToLoad, hiveDatabase, baseTableName, seqColumn, incrementalUBFreeDataframe, currentTimestamp)
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

  def materializeWithLatestVersion(hiveDatabase: String, baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, hiveContext: HiveContext, incrementalDataframe: DataFrame, partitionColumns: String, headerOperation: String, deleteIndicator: String, mandatoryMetaData: Seq[String]): String = {

    val baseDataFrame = if (partitionColumns.size == 0) {
      val baseTableDataframe = hiveContext.table(baseTableName)

      val fieldList = scala.collection.mutable.MutableList[AdditionalFields]()
      var alterIndicator = false
      breakable {
        mandatoryMetaData.foreach(metadata => {
          if(!hasColumn(baseTableDataframe, metadata)) {
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
        println("avroSchemaString"+avroSchemaString)
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
      logger.info("******Base Table with the incremented partitions******* with " + basePartitionsDataframe.count() + " rows")
      basePartitionsDataframe.show()
      basePartitionsDataframe
    }


    val upsertDataframe: DataFrame = getUpsertBaseTableData(hiveContext, baseDataFrame, incrementalDataframe, uniqueKeyList, seqColumn, headerOperation, deleteIndicator)
    logger.info("******Upserted Base Table with the incremented partitions******* with " + upsertDataframe.count() + " rows")
    upsertDataframe.show()

    val baseDataframe = hiveContext.table(baseTableName)
    logger.info("******Initial Base Table with all the partitions******* with " + baseDataframe.count() + " rows")
    baseDataframe.show(50)


    val currentTimestamp = if (partitionColumns.size == 0) {
      writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName, "overwrite", upsertDataframe)
    } else {
      writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "overwrite", upsertDataframe)
    }

    val newBaseDataframe = hiveContext.table(baseTableName)
    logger.info("******Reconciled Base Table******* with " + newBaseDataframe.count() + " rows")
    newBaseDataframe.show(50)

    currentTimestamp

  }


  def buildAvroSchema(hiveDatabase: String, rawSchema: StructType, baseTableName: String, mandatoryMetadataArray: Array[AdditionalFields]) = {
    println("mandatory"+ mandatoryMetadataArray)
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
    val baseDataframe = hiveContext.table(baseTableName)
    logger.info("******Initial Base Table with all the partitions******* with " + baseDataframe.count() + " rows")
    baseDataframe.show(50)

    val currentTimestamp = partitionColumnList match {
      case Nil => writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName, "append", incrementalDataframe)
      case _ => writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "append", incrementalDataframe)
    }

    val newBaseDataframe = hiveContext.table(baseTableName)
    logger.info("******Reconciled Base Table******* with " + newBaseDataframe.count() + " rows")
    newBaseDataframe.show(50)

    currentTimestamp
  }

  def writeUpsertDataBackToBaseTableWithoutPartitions(baseTableName: String, writeMode: String, upsertDataframe: DataFrame): String = {
    upsertDataframe
      .write
      .format("com.databricks.spark.avro")
      .mode(writeMode)
      .insertInto(baseTableName)
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
  }

  def writeUpsertDataBackToBasePartitions(baseTableName: String, partitionColumns: String, writeMode: String, upsertDataframe: DataFrame): String = {
    upsertDataframe
      .write
      .format("com.databricks.spark.avro")
      .mode(writeMode)
      .partitionBy(partitionColumns)
      .insertInto(baseTableName)
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

  def getUpsertBaseTableData(hiveContext: HiveContext, baseTableDataframe: DataFrame, incrementalData: DataFrame, uniqueKeyList: Seq[String], seqColumn: String, headerOperation: String, deleteIndicator: String): DataFrame = {

    val windowFunction = Window.partitionBy(uniqueKeyList.head, uniqueKeyList.tail: _*).orderBy(desc(seqColumn))
    val duplicateFreeIncrementDF = incrementalData.withColumn("rownum", row_number.over(windowFunction)).where("rownum = 1").drop("rownum")
    logger.info("******DuplicateFreeIncrementDF incrementalData Table******* with " + duplicateFreeIncrementDF.count() + " rows")
    duplicateFreeIncrementDF.show()

    val tsAppendedIncDF = duplicateFreeIncrementDF.withColumn("modified_timestamp", lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)))


    /*
    incrementalData.dropDuplicates()
    val upsertsDF = tsAppendedIncDF.except(baseTableDataframe)
    upsertsDF.show()

    val insertedDF= tsAppendedIncDF.filter(tsAppendedIncDF("header__operation").equalTo("UPDATE"))
    insertedDF.show()*/


    val columns = baseTableDataframe.columns
    val incrementDataFrame = tsAppendedIncDF.toDF(tsAppendedIncDF.columns.map(x => x.trim + "_i"): _*)


    val joinExprs = uniqueKeyList
      .zip(uniqueKeyList)
      .map { case (c1, c2) => baseTableDataframe(c1) === incrementDataFrame(c2 + "_i") }
      .reduce(_ && _)

    val joinedDataFrame = baseTableDataframe.join(incrementDataFrame, joinExprs, "outer")
    logger.info("******joinedDataFrame incrementalData Table******* with " + joinedDataFrame.count() + " rows")
    joinedDataFrame.show()


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

    logger.info("******materializedDataframe incrementalData Table******* with " + materializedDataframe.count() + " rows")
    materializedDataframe.show()

    val deleteUpsertFreeDataframe = materializedDataframe.filter(materializedDataframe(headerOperation).notEqual(deleteIndicator))
    deleteUpsertFreeDataframe
  }

  def fillbad = udf((headerOperation: String, baseColumnValue: String, incrementalColValue: String) => {

    val resultantColumnValue: String = headerOperation match {
      case "UPDATE" | "INSERT" => incrementalColValue
      case _ => baseColumnValue
    }
    resultantColumnValue
  })

}
