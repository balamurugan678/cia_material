package com.poc.sample

import com.poc.sample.LoadDataToHive.logger
import com.poc.sample.Models.MaterialConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.hive.HiveContext

object MaterializationCloseDown {

  def dropIncrementalExtTable(materialConfig: MaterialConfig, hiveContext: HiveContext): Unit = {
    hiveContext.sql(s"drop table ${materialConfig.incrementalTableName}")
    logger.warn(s"External table ${materialConfig.incrementalTableName} has been dropped now after successful materialization")
  }

  def moveFilesToProcessedDirectory(materialConfig: MaterialConfig, hadoopConfig: Configuration, hadoopFileSystem: FileSystem): Unit = {
    val status: Array[FileStatus] = hadoopFileSystem.listStatus(new Path(materialConfig.pathToLoad))
    status.foreach(files => {
      FileUtil.copy(hadoopFileSystem, files.getPath, hadoopFileSystem, new Path(materialConfig.processedPathToMove), true, hadoopConfig)
    }
    )
    logger.warn(s"Delta CDC files at ${materialConfig.pathToLoad} have been moved to ${materialConfig.processedPathToMove} now after successful materialization")
  }

}
