package com.poc.sample


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.poc.sample.Models.CIANotification
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, max, _}
import org.apache.spark.sql.hive.HiveContext

object LoadDataToHive {

  def reconcile(pathToLoad: String, hiveDatabase: String, baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, versionIndicator: String, hiveContext: HiveContext): CIANotification = {

    hiveContext.sql(s"use $hiveDatabase")

    val incrementalDataframe = hiveContext.table(incrementalTableName)
    println("******Incremental External Table******* with " + incrementalDataframe.count() + " rows")
    incrementalDataframe.show()

    val partitionColumns = partitionColumnList.mkString(",")
    val ciaNotification: CIANotification = versionIndicator match {
      case "Y" =>
        val currentTimestamp = materializeAndKeepVersion(baseTableName, hiveContext, incrementalDataframe, partitionColumns)
        val notification: CIANotification = buildNotificationObject(pathToLoad, hiveDatabase, baseTableName, seqColumn, incrementalDataframe, currentTimestamp)
        notification
      case "N" =>
        val currentTimestamp = materializeWithLatestVersion(baseTableName, incrementalTableName, uniqueKeyList, partitionColumnList, seqColumn, hiveContext, incrementalDataframe, partitionColumns)
        val notification: CIANotification = buildNotificationObject(pathToLoad, hiveDatabase, baseTableName, seqColumn, incrementalDataframe, currentTimestamp)
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

  def materializeWithLatestVersion(baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, hiveContext: HiveContext, incrementalDataframe: DataFrame, partitionColumns: String): String = {
    val partitionWhereClause: String = getIncrementPartitions(incrementalTableName, partitionColumnList, hiveContext, partitionColumns)
    val basePartitionsDataframe: DataFrame = getBaseTableDataFromIncPartitions(baseTableName, hiveContext, partitionColumns, partitionWhereClause)
    println("******Base Table with the incremented partitions******* with " + basePartitionsDataframe.count() + " rows")
    basePartitionsDataframe.show()

    val upsertDataframe: DataFrame = getUpsertBaseTableData(hiveContext, basePartitionsDataframe, incrementalDataframe, uniqueKeyList, seqColumn)
    println("******Upserted Base Table with the incremented partitions******* with " + upsertDataframe.count() + " rows")
    upsertDataframe.show()

    val baseDataframe = hiveContext.table(baseTableName)
    println("******Initial Base Table with all the partitions******* with " + baseDataframe.count() + " rows")
    baseDataframe.show(50)


    val currentTimestamp = writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "overwrite", upsertDataframe)

    val newBaseDataframe = hiveContext.table(baseTableName)
    println("******Reconciled Base Table******* with " + newBaseDataframe.count() + " rows")
    newBaseDataframe.show(50)

    currentTimestamp
  }

  def materializeAndKeepVersion(baseTableName: String, hiveContext: HiveContext, incrementalDataframe: DataFrame, partitionColumns: String): String = {
    val baseDataframe = hiveContext.table(baseTableName)
    println("******Initial Base Table with all the partitions******* with " + baseDataframe.count() + " rows")
    baseDataframe.show(50)

    val currentTimestamp = writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, "append", incrementalDataframe)

    val newBaseDataframe = hiveContext.table(baseTableName)
    println("******Reconciled Base Table******* with " + newBaseDataframe.count() + " rows")
    newBaseDataframe.show(50)

    currentTimestamp
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

  def getUpsertBaseTableData(hiveContext: HiveContext, baseTableDataframe: DataFrame, incrementalData: DataFrame, uniqueKeyList: Seq[String], seqColumn: String): DataFrame = {
    val columns = baseTableDataframe.columns
    val windowFunction = Window.partitionBy(uniqueKeyList.head, uniqueKeyList.tail: _*).orderBy(desc(seqColumn))
    val duplicateFreeIncrementDF = incrementalData.withColumn("rownum", row_number.over(windowFunction)).where("rownum = 1").drop("rownum")
    println("******DuplicateFreeIncrementDF incrementalData Table******* with " + duplicateFreeIncrementDF.count() + " rows")
    duplicateFreeIncrementDF.show()

    val tsAppendedIncDF = duplicateFreeIncrementDF.withColumn("modified_timestamp", lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)))
    val incrementDataFrame = tsAppendedIncDF.toDF(tsAppendedIncDF.columns.map(x => x.trim + "_i"): _*)


    val joinExprs = uniqueKeyList
      .zip(uniqueKeyList)
      .map { case (c1, c2) => baseTableDataframe(c1) === incrementDataFrame(c2 + "_i") }
      .reduce(_ && _)

    val joinedDataFrame = baseTableDataframe.join(incrementDataFrame, joinExprs, "outer")

    val upsertDataFrame = columns.foldLeft(joinedDataFrame) {
      (acc: DataFrame, colName: String) =>
        acc.withColumn(colName + "_j", coalesce(col(colName + "_i"), col(colName)))
          .drop(colName)
          .drop(colName + "_i")
          .withColumnRenamed(colName + "_j", colName)
    }

    upsertDataFrame
  }

}
