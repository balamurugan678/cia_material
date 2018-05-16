package com.poc.sample

import org.apache.spark.sql.hive.HiveContext

object IncrementalTableSetUp {

  def loadIncrementalData(pathToLoad: String, hiveDatabase: String, incrementalTableName: String, hiveContext: HiveContext): Unit = {

    val incrementalData = hiveContext
      .read
      .format("com.databricks.spark.avro")
      .load(pathToLoad)

    val rawSchema = incrementalData.schema

    val schemaString = rawSchema.fields.map(field => field.name.toLowerCase().replaceAll("""^_""", "").concat(" ").concat(field.dataType.typeName match {
      case "integer" | "Long" | "long" => "bigint"
      case others => others
    })).mkString(",")

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
         |TBLPROPERTIES('avro.schema.literal' = '{
         |  "type" : "record",
         |  "name" : "MyClass",
         |  "namespace" : "com.test.avro",
         |  "fields" : [ {
         |    "name" : "emplo00001",
         |    "aliases":["EMPLO00001"],
         |    "type" : "long"
         |  }, {
         |    "name" : "first_name",
         |    "aliases":["FIRST_NAME"],
         |    "type" : "string"
         |  }, {
         |    "name" : "secon00001",
         |    "aliases":["SECON00001"],
         |    "type" : "string"
         |  }, {
         |    "name" : "age",
         |    "aliases":["AGE"],
         |    "type" : "string"
         |  }, {
         |    "name" : "salary",
         |    "aliases":["SALARY"],
         |    "type" : "string"
         |  }, {
         |    "name" : "dept",
         |    "aliases":["DEPT"],
         |    "type" : "string"
         |  }, {
         |    "name" : "a_enttyp",
         |    "aliases":["A_ENTTYP"],
         |    "type" : "string"
         |  }, {
         |    "name" : "a_timestamp",
         |    "aliases":["A_TIMESTAMP"],
         |    "type" : "string"
         |  }, {
         |    "name" : "a_seqno",
         |     "aliases":["A_SEQNO"],
         |    "type" : "string"
         |  }, {
         |    "name" : "a_system",
         |     "aliases":["A_SYSTEM"],
         |    "type" : "string"
         |  }, {
         |    "name" : "a_user",
         |     "aliases":["A_USER"],
         |    "type" : "string"
         |  } ]
         |}')
       """.stripMargin

    hiveContext.sql(incrementalExtTable)
  }

}
