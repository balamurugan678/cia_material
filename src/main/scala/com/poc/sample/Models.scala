package com.poc.sample

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}

object Models {

  case class CIAMaterialConfig(appName: String, environment: String, clusterName: String, sourceName: String, kerberosPrincipal: String, kerberosKeyTabLocation: String, esStatusIndicator: Boolean, esIndex: String, schemaDirectory: String, attunityCDCIndicator: Boolean, cdcJournalControlFields: String,
                               createBaseTable: Boolean, createBaseTableFromScooped: Boolean, incrementalHiveTableExist: Boolean, seqColumn: String, headerOperation: String, deleteIndicator: String,
                               beforeImageIndicator: String, mandatoryMetaData: String, overrideIndicator: Boolean, materialConfigs: List[MaterialConfig]
                              )

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, createBaseTable: Boolean, createBaseTableFromScooped: Boolean, incrementalHiveTableExist: Boolean, incrementalTableName: String,
                            pathToLoad: String, attunityUnpackedPath: String, attunityUnpackedArchive: String,
                            processedPathToMove: String, uniqueKeyList: String, partitionColumns: String, seqColumn: String,
                            headerOperation: String, deleteIndicator: String, beforeImageIndicator: String, mandatoryMetaData: String)

  case class Item(id: String, name: String)

  case class CIANotification(sourceName: String, componentName: String, hiveDatabase: String, baseTableName: String, incrementalPathLocation: String, latestTimestamp: String, currentTimestamp: String, noOfRecords: Long, timeTaken: String) {
    override def toString = "Source Name = " + sourceName + " Component Name =" + componentName + " Hive database = " + hiveDatabase + " Base Table name = " + baseTableName +
      " Incremental Path = " + incrementalPathLocation + " Latest timestamp in Records = " + latestTimestamp + " Current Timestamp = " + currentTimestamp +
      " No of Records " + noOfRecords + " Time taken " + timeTaken
  }

  case class AvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[Fields])

  case class MatDecAvroSchema(@JsonProperty("type") typz: String, name: String, namespace: String, fields: Array[BaseAvroSchema])

  case class Fields(name: String, alias: String, @JsonProperty("type") typz: String)

  case class BaseAvroSchema(name: String, alias: String, @JsonProperty("type") typz: (String, AdditionalFields), default: String)

  case class AdditionalFields(@JsonProperty("type") typz: String, @JsonInclude(Include.NON_NULL) logicalType: String, @JsonInclude(Include.NON_NULL) precision: Int, @JsonInclude(Include.NON_NULL) scale: Int)

}
