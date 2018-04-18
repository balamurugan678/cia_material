package com.poc.sample

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.hive.HiveContext

object MaterializationCloseDown {

  def dropIncrementalExtTable(incrementalTableName: String, hiveContext: HiveContext) : Unit = {
    hiveContext.sql(s"drop table $incrementalTableName")
  }

  def moveFilesToProcessedDirectory(hadoopConfig: Configuration, hadoopFileSystem: FileSystem, pathToLoad: String, processedPathToMove:String) : Unit = {
    val status: Array[FileStatus] = hadoopFileSystem.listStatus(new Path(pathToLoad))
    status.foreach(files => {
      FileUtil.copy(hadoopFileSystem, files.getPath, hadoopFileSystem, new Path(processedPathToMove), true, hadoopConfig)
    }
    )
  }

}
