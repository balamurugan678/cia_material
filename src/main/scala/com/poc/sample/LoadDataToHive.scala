package com.poc.sample


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, _}
import org.apache.spark.sql.hive.HiveContext

object LoadDataToHive {

  def reconcile(hiveDatabase: String, baseTableName: String, incrementalTableName: String, uniqueKeyList: Seq[String], partitionColumnList: Seq[String], seqColumn: String, versionIndicator:String, hiveContext: HiveContext): Unit = {

    hiveContext.sql(s"use $hiveDatabase")

    val incrementalDataframe = hiveContext.table(incrementalTableName)
    println("******Incremental External Table******* with " + incrementalDataframe.count() + " rows")
    incrementalDataframe.show()

    val partitionColumns = partitionColumnList.mkString(",")
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

    val writeMode = if (versionIndicator == "Y") "append" else "overwrite"
    writeUpsertDataBackToBasePartitions(baseTableName, partitionColumns, writeMode, upsertDataframe)

    val newBaseDataframe = hiveContext.table(baseTableName)
    println("******Reconciled Base Table******* with " + newBaseDataframe.count() + " rows")
    newBaseDataframe.show(50)
  }


  def writeUpsertDataBackToBasePartitions(baseTableName: String, partitionColumns: String, writeMode:String, upsertDataframe: DataFrame) = {
    upsertDataframe
      .write
      .format("com.databricks.spark.avro")
      .mode(writeMode)
      .partitionBy(partitionColumns)
      .insertInto(baseTableName)
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

    val incrementDataFrame = duplicateFreeIncrementDF.toDF(duplicateFreeIncrementDF.columns.map(x => x.trim + "_i"): _*)

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
