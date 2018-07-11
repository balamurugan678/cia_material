package com.poc.sample


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, _}
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by 44046053 on 06/07/2018.
  */
object AttunityDataUnpack {

  def unPackEncapsulatedAttunityMessage(hadoopFileSystem: FileSystem, hadoopConfig: Configuration, hiveContext: HiveContext, pathToLoad: String, attunityUnpackedFilePath: String, attunityProcessedFilePath: String): Unit = {

    val columnNames = Seq("message.data.*", "message.headers.*")
    val sourceFiles = hadoopFileSystem.listStatus(new Path(pathToLoad))
    sourceFiles.foreach(sourceFile => {
      val attunityAvroDataframe = hiveContext
        .read
        .format("com.databricks.spark.avro")
        .load(sourceFile.getPath().toString)

      val attunityUpperDF = attunityAvroDataframe.select(columnNames.head, columnNames.tail: _*)
        .withColumnRenamed("operation", "header__operation")
        .withColumnRenamed("changeSequence", "header__changeSequence")
        .withColumnRenamed("timestamp", "header__timestamp")
        .withColumnRenamed("streamPosition", "header__streamPosition")
        .withColumnRenamed("transactionId", "header__transactionId")

      val attunityDF = attunityUpperDF.toDF(attunityUpperDF.columns map (_.toLowerCase): _*)
      attunityDF.show()
      attunityDF.write
        .mode("append")
        .format("com.databricks.spark.avro")
        .save(attunityUnpackedFilePath)

    })

    val status: Array[FileStatus] = hadoopFileSystem.listStatus(new Path(pathToLoad))
    status.foreach(files => {
      FileUtil.copy(hadoopFileSystem, files.getPath, hadoopFileSystem, new Path(attunityProcessedFilePath), true, hadoopConfig)
    })

  }

}
