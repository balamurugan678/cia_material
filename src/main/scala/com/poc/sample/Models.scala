package com.poc.sample

import com.fasterxml.jackson.annotation.JsonProperty

object Models {

  case class CIAMaterialConfig(appName: String, environment: String, kerberosPrincipal: String, kerberosKeyTabLocation: String, esStatusIndicator: Boolean, esHost: String, esPort: String, esIndex: String, cdcJournalControlFields: String,
                               createBaseTable: Boolean, createIncrementalTable: Boolean, seqColumn: String, versionIndicator: String, headerOperation: String, deleteIndicator: String,
                               beforeImageIndicator: String, mandatoryMetaData: String, overrideIndicator: String, materialConfigs: List[MaterialConfig]
                              )

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, createBaseTable: Boolean, createIncrementalTable: Boolean, incrementalTableName: String, pathToLoad: String, processedPathToMove: String,
                            uniqueKeyList: String, partitionColumns: String, seqColumn: String, versionIndicator: String,
                            headerOperation: String, deleteIndicator: String, beforeImageIndicator: String, mandatoryMetaData: String)

  case class Item(id: String, name: String)

  case class CIANotification(hiveDatabase: String, baseTableName: String, incrementalPathLocation: String, latestTimestamp: String, currentTimestamp: String)

  case class AvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[Fields])

  case class Fields(name: String, alias: String, @JsonProperty("type") typz: String)

  case class BaseAvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[AdditionalFields])

  case class AdditionalFields(name: String, alias: String, @JsonProperty("type") typz: Array[String])

}
