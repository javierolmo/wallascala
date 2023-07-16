package com.javi.personal.wallascala

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate

trait SparkUtils {

  protected def ymdCondition(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
      col("month") === lit(date.getMonthValue) &&
      col("day") === lit(date.getDayOfMonth)

  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .format("parquet")
      .load(PathBuilder.buildSanitedPath(source, datasetName).url)

  protected def readProcessed(datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    readProcessed(datasetName).filter(ymdCondition(date))

  protected def readProcessed(datasetName: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .format("parquet")
      .load(PathBuilder.buildProcessedPath(datasetName).url)

}
