package com.javi.personal.wallascala

import com.javi.personal.wallascala.processor.ProcessedTables
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

trait SparkUtils {

  @deprecated("")
  protected def ymdCondition(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
      col("month") === lit(date.getMonthValue) &&
      col("day") === lit(date.getDayOfMonth)

  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName))

  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    read(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  protected def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    readOptional(PathBuilder.buildSanitedPath(source, datasetName).cd(date))

  protected def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    read(location)
  }

  protected def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = dateOption.map(date => PathBuilder.buildProcessedPath(dataset.getName).cd(date))
      .getOrElse(PathBuilder.buildProcessedPath(dataset.getName))
    readOptional(location)
  }

  private def read(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).load(location.url)

  private def readOptional(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): Option[DataFrame] =
    try Some(read(location, format)) catch { case _: Exception => None }


  implicit class DataFrameOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }

}
