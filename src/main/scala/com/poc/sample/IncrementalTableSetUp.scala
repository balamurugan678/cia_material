package com.poc.sample

import org.apache.spark.sql.hive.HiveContext

object IncrementalTableSetUp {

  def loadIncrementalData(pathToLoad: String, hiveDatabase: String, incrementalTableName: String, hiveContext: HiveContext): Unit = {

    val incrementalData = hiveContext
      .read
      .format("com.databricks.spark.avro")
      .load(pathToLoad)

    val rawSchema = incrementalData.schema

    val schemaString = rawSchema.fields.map(field => field.name.replaceAll("""^_""", "").concat(" ").concat(field.dataType.typeName match {
      case "integer" => "int"
      case "Long" | "long" => "bigint"
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
       """.stripMargin

    hiveContext.sql(incrementalExtTable)

  }

}
