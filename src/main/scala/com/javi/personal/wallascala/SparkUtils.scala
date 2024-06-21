package com.javi.personal.wallascala

import com.javi.personal.wallascala.processor.ProcessedTables
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
    read(location)
  }

  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
    val location = PathBuilder.buildSanitedPath(source, datasetName).cd(date)
    read(location)
  }

  protected def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = PathBuilder.buildSanitedPath(source, datasetName).cd(date)
    readOptional(location)
  }

  protected def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = Option.empty)(implicit spark: SparkSession): DataFrame = {
    val location = dateOption match {
      case Some(date) => PathBuilder.buildProcessedPath(dataset.getName).cd(date)
      case None => PathBuilder.buildProcessedPath(dataset.getName)
    }
    read(location)
  }

  protected def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = Option.empty)(implicit spark: SparkSession): Option[DataFrame] = {
    val location = dateOption match {
      case Some(date) => PathBuilder.buildProcessedPath(dataset.getName).cd(date)
      case None => PathBuilder.buildProcessedPath(dataset.getName)
    }
    readOptional(location)
  }

  private def read(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).load(location.url)

  private def readOptional(location: StorageAccountLocation, format: String = "parquet")(implicit spark: SparkSession): Option[DataFrame] = try {
    Some(read(location, format))
  } catch {
    case _: Exception => None
  }


  implicit class DataFrameOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }

}
