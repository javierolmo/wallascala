package com.javi.personal.wallascala.utils

import com.javi.personal.wallascala.processor.ProcessedTables
import com.javi.personal.wallascala.{PathBuilder, StorageAccountLocation}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class DefaultDataSourceProvider extends DataSourceProvider {

  override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName))

  override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    readOption(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    read(location)
  }

  override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    readOption(location)
  }

  private def read(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).load(location.url)

  private def readOption(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): Option[DataFrame] =
    try Some(read(location, format)) catch { case _: Exception => None }

}
