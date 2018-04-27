package com.poc.sample

object Models {

  case class CIAMaterialConfig(appName: String, esHost: String, esPort: String, esIndex: String, materialConfigs: List[MaterialConfig])

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, incrementalTableName: String, pathToLoad: String, processedPathToMove: String, uniqueKeyList: String, partitionColumns: String, seqColumn: String, versionIndicator: String)

  case class Item(id: String, name: String)

  case class CIANotification(hiveDatabase: String, baseTableName: String, incrementalPathLocation: String, latestTimestamp: String, currentTimestamp: String)

}
