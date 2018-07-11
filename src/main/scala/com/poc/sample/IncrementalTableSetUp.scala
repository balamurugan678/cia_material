package com.poc.sample

import java.io.FileNotFoundException

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.poc.sample.Models._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object IncrementalTableSetUp {

  val logger = LoggerFactory.getLogger(IncrementalTableSetUp.getClass)

  def loadIncrementalData(hadoopFileSystem: FileSystem, hadoopConfig: Configuration, materialConfig: MaterialConfig, ciaMaterialConfig: CIAMaterialConfig, hiveContext: HiveContext, controlFields: Seq[String]): Try[String] = {

    if (materialConfig.createIncrementalTable)
      return Success("Success")

    val hiveDatabase = materialConfig.hiveDatabase
    val baseTableName = materialConfig.baseTableName
    val incrementalTableName = materialConfig.incrementalTableName
    val pathToLoad = materialConfig.pathToLoad
    val shouldCreateBaseTable = materialConfig.createBaseTable
    val attunityUnpackedPath = materialConfig.attunityUnpackedPath
    val attunityUnpackedArchive = materialConfig.attunityUnpackedArchive
    try {

      if (ciaMaterialConfig.attunityCDCIndicator) {
        AttunityDataUnpack.unPackEncapsulatedAttunityMessage(hadoopFileSystem, hadoopConfig, hiveContext, pathToLoad, attunityUnpackedPath, attunityUnpackedArchive)
        logger.warn(s"Encapsulated Files at $incrementalTableName have been unpacked and unpacked files are placed at $pathToLoad")
      }

      val incrementalFilesPathToLoad =
        if (ciaMaterialConfig.attunityCDCIndicator)
          attunityUnpackedPath
        else
          pathToLoad

      val incrementalData = hiveContext
        .read
        .format("com.databricks.spark.avro")
        .load(incrementalFilesPathToLoad)

      val rawSchema = incrementalData.schema

      if (shouldCreateBaseTable)
        createBaseTable(materialConfig, hiveContext, rawSchema, controlFields)

      val schemaString = rawSchema.fields.map(field => field.name.toLowerCase().replaceAll("""^_""", "").concat(" ").concat(field.dataType.typeName match {
        case "integer" | "Long" | "long" => "bigint"
        case others => others
      })).mkString(",")
      val avroSchemaString: String = buildAvroSchema(hiveDatabase, rawSchema, baseTableName)

      hiveContext.sql(s"USE $hiveDatabase")
      hiveContext.sql(s"DROP TABLE IF EXISTS $incrementalTableName")
      val incrementalExtTable =
        s"""
           |Create external table $incrementalTableName ($schemaString)\n
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \n
           | Stored As Avro \n
           |-- inputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n
           | -- outputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n
           |LOCATION '$incrementalFilesPathToLoad' \n
           |TBLPROPERTIES('avro.schema.literal' = '$avroSchemaString')
       """.stripMargin
      hiveContext.sql(incrementalExtTable)
      logger.warn(s"Incremental external table has been created with the name ${incrementalTableName} and the delta files have been loaded from ${pathToLoad}")
      Success("Success")
    }
    catch {
      case ex: FileNotFoundException => Failure(ex)
    }
  }

  def createBaseTable(materialConfig: MaterialConfig, hiveContext: HiveContext, rawSchema: StructType, controlFields: Seq[String]): Try[String] = {
    val hiveDatabase = materialConfig.hiveDatabase
    val baseTableName = materialConfig.baseTableName
    val controlFieldsToRemove = controlFields.filter(field => !materialConfig.mandatoryMetaData.contains(field.toLowerCase))
    val baseTableSchemaFields = rawSchema.fields.filter { field =>
      !controlFieldsToRemove.contains(field.name.toLowerCase)
    }

    try {
      val baseTableSchemaAsString = buildBaseTableAvroSchema(hiveDatabase, baseTableSchemaFields, baseTableName)
      hiveContext.sql(s"USE $hiveDatabase")
      hiveContext.sql(s"DROP TABLE IF EXISTS $baseTableName")
      val baseTable =
        s"""
           |Create table $baseTableName \n
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \n
           | Stored As Avro \n
           |-- inputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \n
           | -- outputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \n
           |TBLPROPERTIES('avro.schema.literal' = '$baseTableSchemaAsString')
       """.stripMargin
      hiveContext.sql(baseTable)
      logger.debug(s"Base external table has been created with the name ${baseTable}")
      Success("Success")
    } catch {
      case ex: FileNotFoundException => Failure(ex)
    }
  }

  def buildBaseTableAvroSchema(hiveDatabase: String, baseTableSchemaFields: Array[StructField], baseTableName: String) = {
    val schemaList: Array[BaseAvroSchema] = baseTableSchemaFields.map(field => BaseAvroSchema(field.name, field.name, ("null", buildDecimalSchema(field.dataType.typeName)), null))
    buildBaseAvroSchemaJsonString(hiveDatabase, baseTableName, schemaList)
  }

  def buildAvroSchema(hiveDatabase: String, rawSchema: StructType, baseTableName: String) = {
    val schemaList = rawSchema.fields.map(field => BaseAvroSchema(field.name, field.name, ("null", buildDecimalSchema(field.dataType.typeName)), null))
    buildBaseAvroSchemaJsonString(hiveDatabase, baseTableName, schemaList)
  }

  def buildDecimalSchema(decimalTypeString: String): AdditionalFields = {
    if (decimalTypeString.contains("decimal")) {
      val logicalType = decimalTypeString.substring(0, decimalTypeString.indexOf("("))
      val precision = decimalTypeString.substring(decimalTypeString.indexOf("(") + 1, decimalTypeString.indexOf(",")).toInt
      val scale = decimalTypeString.substring(decimalTypeString.indexOf(",") + 1, decimalTypeString.indexOf(")")).toInt
      AdditionalFields("bytes", logicalType, precision, scale)
    }
    else {
      decimalTypeString match {
        case "integer" => AdditionalFields("int", null, 0, 0)
        case others => AdditionalFields(decimalTypeString, null, 0, 0)
      }
    }
  }

  def buildBaseAvroSchemaJsonString(hiveDatabase: String, tableName: String, schemaList: Array[BaseAvroSchema]): String = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val avroSchema = MatDecAvroSchema("record", tableName, hiveDatabase, schemaList)
    val avroSchemaString = mapper.writeValueAsString(avroSchema)
    avroSchemaString
  }

}
