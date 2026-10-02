package com.javi.personal.tfg.processors.utils

import com.javi.personal.tfg.processors.processor.ProcessedTables
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

trait DataSourceProvider {
  def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame
  def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame
  def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame]
  def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame
  def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame]
}
