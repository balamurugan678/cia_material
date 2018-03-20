package com.poc.sample

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object IncrementalRunner {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("IncrementalRunner")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    val hiveContext = new HiveContext(sparkContext)

    Source.fromFile(sparkContext.getConf.get("spark.configFileLocation"))
      .getLines
      .foreach(line => {

        val configList = line.split("~")
        val hiveDatabase = configList(0)
        val baseTableName = configList(1)
        val incrementalTableName = configList(2)
        val pathToLoad = configList(3)
        val uniqueKeyList = configList(4).split('|').toSeq
        val partitionColumns = configList(5).split('|').toSeq

        IncrementalTableSetUp.loadIncrementalData(pathToLoad, hiveDatabase, incrementalTableName, hiveContext)

        LoadDataToHive.reconcile(hiveDatabase, baseTableName, incrementalTableName, uniqueKeyList, partitionColumns, hiveContext)

        hiveContext.sql(s"drop table $incrementalTableName")

      })


  }

}
