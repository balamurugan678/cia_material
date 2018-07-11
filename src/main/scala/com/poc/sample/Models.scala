package com.poc.sample

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}

object Models {

  case class CIAMaterialConfig(appName: String, environment: String, kerberosPrincipal: String, kerberosKeyTabLocation: String, esStatusIndicator: Boolean, esHost: String,
                               esPort: String, esIndex: String, attunityCDCIndicator: Boolean, cdcJournalControlFields: String,
                               createBaseTable: Boolean, createIncrementalTable: Boolean, seqColumn: String, headerOperation: String, deleteIndicator: String,
                               beforeImageIndicator: String, mandatoryMetaData: String, overrideIndicator: String, materialConfigs: List[MaterialConfig]
                              )

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, createBaseTable: Boolean, createIncrementalTable: Boolean, incrementalTableName: String,
                            pathToLoad: String, attunityUnpackedPath: String, attunityUnpackedArchive: String,
                            processedPathToMove: String, uniqueKeyList: String, partitionColumns: String, seqColumn: String,
                            headerOperation: String, deleteIndicator: String, beforeImageIndicator: String, mandatoryMetaData: String)

  case class Item(id: String, name: String)

  case class CIANotification(hiveDatabase: String, baseTableName: String, incrementalPathLocation: String, latestTimestamp: String, currentTimestamp: String)

  case class AvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[Fields])

  case class MatDecAvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[BaseAvroSchema])

  case class Fields(name: String, alias: String, @JsonProperty("type") typz: String)

  case class BaseAvroSchema(name: String, alias: String, @JsonProperty("type") typz: (String, AdditionalFields), default: String)

  case class AdditionalFields(@JsonProperty("type") typz: String, @JsonInclude(Include.NON_NULL) logicalType: String, @JsonInclude(Include.NON_NULL) precision: Int, @JsonInclude(Include.NON_NULL) scale: Int)

}
