package com.javi.personal.wallascala.utils

import com.javi.personal.wallascala.processor.ProcessedTables
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

trait DataSourceProvider {
  def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame
  def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame
  def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame]
  def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame
  def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame]
}
