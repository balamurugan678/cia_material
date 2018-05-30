package com.poc.sample

import java.io.FileNotFoundException

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.poc.sample.Models.{AvroSchema, Fields, MaterialConfig}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object IncrementalTableSetUp {

  val logger = LoggerFactory.getLogger(IncrementalTableSetUp.getClass)

  def loadIncrementalData(materialConfig: MaterialConfig, hiveContext: HiveContext): Try[String] = {

    val hiveDatabase = materialConfig.hiveDatabase
    val baseTableName = materialConfig.baseTableName
    val incrementalTableName = materialConfig.incrementalTableName
    val pathToLoad = materialConfig.pathToLoad

    try {
      val incrementalData = hiveContext
        .read
        .format("com.databricks.spark.avro")
        .load(pathToLoad)

      val rawSchema = incrementalData.schema
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
           |LOCATION '$pathToLoad' \n
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


  def buildAvroSchema(hiveDatabase: String, rawSchema: StructType, baseTableName: String) = {
    val schemaList = rawSchema.fields.map(field => Fields(field.name, field.name, field.dataType.typeName match {
      case "integer" => "int"
      case others => others
    }))
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val avroSchema = AvroSchema("record", baseTableName, hiveDatabase, schemaList)
    val avroSchemaString = mapper.writeValueAsString(avroSchema)
    avroSchemaString
  }
}
