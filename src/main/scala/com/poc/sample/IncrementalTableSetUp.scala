package com.poc.sample

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.poc.sample.Models.{AvroSchema, Fields}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

object IncrementalTableSetUp {

  def loadIncrementalData(pathToLoad: String, hiveDatabase: String, baseTableName: String, incrementalTableName: String, hiveContext: HiveContext): Unit = {

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

  }


  def buildAvroSchema(hiveDatabase: String, rawSchema: StructType, baseTableName: String) = {
    val schemaList = rawSchema.fields.map(field => Fields(field.name, field.name, field.dataType.typeName))
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val avroSchema = AvroSchema("record", baseTableName, hiveDatabase, schemaList)
    val avroSchemaString = mapper.writeValueAsString(avroSchema)
    avroSchemaString
  }
}
