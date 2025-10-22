package com.javi.personal.wallascala.utils.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkFileReader
  (path: String, format: String = "parquet", options: Map[String, String] = Map(), schema: Option[StructType] = None)
  (implicit spark: SparkSession)
extends SparkReader (format=format, options=options, schema=schema) {

  override def read(): DataFrame = baseReader().load(path)

}

object SparkFileReader {

  def read(path: String, format: String = "parquet", options: Map[String, String] = Map(), schema: Option[StructType] = None) (implicit spark: SparkSession): DataFrame =
    new SparkFileReader(path=path, format=format, options=options, schema=schema).read()

}