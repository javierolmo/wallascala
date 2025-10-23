package com.javi.personal.wallascala.processor

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import scala.collection.mutable

class MockDataSourceProvider extends DataSourceProvider {

  private val sanitedDataSources = mutable.Map[(String, String, Option[LocalDate]), DataFrame]()
  private val processedDataSources = mutable.Map[(ProcessedTables, Option[LocalDate]), DataFrame]()

  def registerSanitedDataSource(source: String, datasetName: String, date: Option[LocalDate], dataFrame: DataFrame): Unit = {
    sanitedDataSources.put((source, datasetName, date), dataFrame)
  }

  def registerProcessedDataSource(dataset: ProcessedTables, date: Option[LocalDate], dataFrame: DataFrame): Unit = {
    processedDataSources.put((dataset, date), dataFrame)
  }

  override def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = {
    sanitedDataSources.getOrElse((source, datasetName, None), 
      throw new RuntimeException(s"No mock data registered for sanited source: $source, dataset: $datasetName"))
  }

  override def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
    sanitedDataSources.getOrElse((source, datasetName, Some(date)), 
      throw new RuntimeException(s"No mock data registered for sanited source: $source, dataset: $datasetName, date: $date"))
  }

  override def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = {
    sanitedDataSources.get((source, datasetName, Some(date)))
  }

  override def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame = {
    processedDataSources.getOrElse((dataset, dateOption), 
      throw new RuntimeException(s"No mock data registered for processed dataset: ${dataset.getName}, date: $dateOption"))
  }

  override def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] = {
    processedDataSources.get((dataset, dateOption))
  }
}
