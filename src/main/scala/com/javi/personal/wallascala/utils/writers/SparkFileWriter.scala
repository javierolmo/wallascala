package com.javi.personal.wallascala.utils.writers

import com.javi.personal.wallascala.PathBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class SparkFileWriter
  (path: String, hiveTable: Option[String], format: String = "parquet", saveMode: String = "overwrite", options: Map[String, String] = Map(), coalesce: Option[Int] = Option.empty)
  (implicit spark: SparkSession)
extends SparkWriter(format=format, saveMode=saveMode, options=options) {

  private def withCoalesce(dataFrame: DataFrame): DataFrame = coalesce match {
    case Some(value) => dataFrame.coalesce(value)
    case None => dataFrame
  }

  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    val writer = baseWriter(withCoalesce(dataFrame))
    hiveTable match {
      case Some(value) => writer.option("path", path).saveAsTable(value)
      case None => writer.save(path)
    }
  }
}

object SparkFileWriter {

  def write(dataFrame: DataFrame, path: String, hiveTable: Option[String], format: String = "parquet", saveMode: String = "overwrite", options: Map[String, String] = Map(), coalesce: Option[Int] = Option.empty) (implicit spark: SparkSession): Unit =
    SparkFileWriter(path, hiveTable, format, saveMode, options, coalesce).write(dataFrame)

  def writeSanited(dataFrame: DataFrame, source: String, datasetName: String, date: Option[LocalDate] = Option.empty)(implicit spark: SparkSession): Unit = {
    val baseLocation = PathBuilder.buildSanitedPath(source, datasetName)
    val location = date match {
      case Some(value) => baseLocation.cd(value)
      case None => baseLocation
    }
    write(dataFrame, location.url, Some(f"sanited.${source}_$datasetName"))
  }

  def writeExcluded(dataFrame: DataFrame, source: String, datasetName: String, date: Option[LocalDate] = Option.empty)(implicit spark: SparkSession): Unit = {
    val baseLocation = PathBuilder.buildExcludedPath(source, datasetName)
    val location = date match {
      case Some(value) => baseLocation.cd(value)
      case None => baseLocation
    }
    write(dataFrame, location.url, Some(f"excluded.${source}_$datasetName"))
  }

}
