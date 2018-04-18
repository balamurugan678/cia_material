package com.poc.sample

object Models {

  case class CIAMaterialConfig(materialConfigs: List[MaterialConfig])
  case class MaterialConfig(hiveDatabase:String, baseTableName:String, incrementalTableName:String, pathToLoad:String, processedPathToMove:String, uniqueKeyList:String, partitionColumns:String, seqColumn:String, versionIndicator:String)

}
