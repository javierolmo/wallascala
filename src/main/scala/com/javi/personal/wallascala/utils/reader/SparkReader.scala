package com.javi.personal.wallascala.utils.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

abstract class SparkReader
  (format: String, options: Map[String, String], schema: Option[StructType])
  (implicit spark: SparkSession)
{

  protected def baseReader(): DataFrameReader = {
    val reader = spark.read.format(format).options(options)
    schema.map(reader.schema).getOrElse(reader)
  }

  def read(): DataFrame

}

object SparkReader {

  def flattenFields(dataFrame: DataFrame): DataFrame = {
    val flattenedColumns = dataFrame.schema.fields.flatMap { field =>
      field.dataType match {
        case structType: StructType =>
          structType.fields.map(innerField => col(s"${field.name}.${innerField.name}").alias(s"${field.name}__${innerField.name}"))
        case _ =>
          Array(col(field.name))
      }
    }
    dataFrame.select(flattenedColumns: _*)
  }
}
