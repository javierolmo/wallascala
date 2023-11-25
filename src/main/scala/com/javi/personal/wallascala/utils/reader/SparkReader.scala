package com.javi.personal.wallascala.utils.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

abstract class SparkReader
  (format: String, options: Map[String, String], schema: Option[StructType])
  (implicit spark: SparkSession)
{

  protected def baseReader(): DataFrameReader = {
    val reader = spark.read.format(format).options(options)
    schema match {
      case Some(value) => reader.schema(value)
      case None => reader
    }
  }

  def read(): DataFrame

}
