package com.javi.personal.wallascala

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate

trait SparkUtils {

  @deprecated("")
  protected def ymdCondition(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
      col("month") === lit(date.getMonthValue) &&
      col("day") === lit(date.getDayOfMonth)

  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = {
    val location = PathBuilder.buildSanitedPath(source, datasetName)
    readParquet(location)
  }

  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
    val location = PathBuilder.buildSanitedPath(source, datasetName).cd(date)
    readParquet(location)
  }

  protected def readProcessed(datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
    val location = PathBuilder.buildProcessedPath(datasetName).cd(date)
    readParquet(location)
  }

  protected def readProcessed(datasetName: String)(implicit spark: SparkSession): DataFrame = {
    val location = PathBuilder.buildProcessedPath(datasetName)
    readParquet(location)
  }

  private def readParquet(location: StorageAccountLocation)(implicit spark: SparkSession): DataFrame =
    spark.read.format("parquet").load(location.url)

  implicit class DataFrameOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }

}
