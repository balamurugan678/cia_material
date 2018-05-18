package com.poc.sample

import com.fasterxml.jackson.annotation.JsonProperty

object Models {

  case class CIAMaterialConfig(appName: String, esHost: String, esPort: String, esIndex: String, materialConfigs: List[MaterialConfig])

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, incrementalTableName: String, pathToLoad: String, processedPathToMove: String,
                            uniqueKeyList: String, partitionColumns: String, seqColumn: String, versionIndicator: String,
                            headerOperation: String, deleteIndicator: String, mandatoryMetaData: String)

  case class Item(id: String, name: String)

  case class CIANotification(hiveDatabase: String, baseTableName: String, incrementalPathLocation: String, latestTimestamp: String, currentTimestamp: String)

  case class AvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[Fields])

  case class Fields(name: String, alias: String, @JsonProperty("type") typz: String)

  case class BaseAvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[AdditionalFields])

  case class AdditionalFields(name: String, alias: String, @JsonProperty("type") typz: Array[String])

}
