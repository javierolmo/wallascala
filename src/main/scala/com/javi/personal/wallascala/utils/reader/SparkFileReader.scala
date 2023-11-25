package com.javi.personal.wallascala.utils.reader

import com.javi.personal.wallascala.PathBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class SparkFileReader
  (path: String, format: String = "parquet", options: Map[String, String] = Map(), schema: Option[StructType] = Option.empty)
  (implicit spark: SparkSession)
extends SparkReader (format=format, options=options, schema=schema) {

  override def read(): DataFrame = baseReader().load(path)

}

object SparkFileReader {

  def read(path: String, format: String = "parquet", options: Map[String, String] = Map(), schema: Option[StructType] = Option.empty) (implicit spark: SparkSession): DataFrame =
    new SparkFileReader(path=path, format=format, options=options, schema=schema).read()

  def readRaw(source: String, datasetName: String, date: Option[LocalDate] = Option.empty) (implicit spark: SparkSession): DataFrame = {
    val baseLocation = PathBuilder.buildRawPath(source, datasetName)
    val location = date match {
      case Some(value) => baseLocation.cd(value)
      case None => baseLocation
    }
    read(path = location.url)
  }

}