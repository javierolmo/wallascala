package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.{PathBuilder, StorageAccountLocation}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class DefaultDataSourceProvider extends DataSourceProvider {

  override def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName))

  override def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  override def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    readOptional(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  override def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    read(location)
  }

  override def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    readOptional(location)
  }

  private def read(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).load(location.url)

  private def readOptional(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): Option[DataFrame] =
    try Some(read(location, format)) catch { case _: Exception => None }

}
