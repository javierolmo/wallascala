package com.javi.personal.wallascala

import com.javi.personal.wallascala.processor.{DataSourceProvider, DefaultDataSourceProvider, ProcessedTables}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

trait SparkUtils {

  protected val dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider()

  @deprecated("")
  protected def ymdCondition(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
      col("month") === lit(date.getMonthValue) &&
      col("day") === lit(date.getDayOfMonth)

  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName)

  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName, date)

  protected def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readSanitedOptional(source, datasetName, date)

  protected def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readProcessed(dataset, dateOption)

  protected def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readProcessedOptional(dataset, dateOption)


  implicit class DataFrameOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }

}
