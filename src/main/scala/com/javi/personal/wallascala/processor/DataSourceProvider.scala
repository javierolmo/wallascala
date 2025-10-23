package com.javi.personal.wallascala.processor

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

/**
 * Interface for reading data from various sources in the data pipeline.
 * Implementations should handle the specifics of data location and access patterns.
 * 
 * This abstraction allows for:
 * - Testing with mock data sources
 * - Switching between storage systems
 * - Consistent data access patterns across the application
 */
trait DataSourceProvider {
  
  /**
   * Reads sanitized data from a source without date filtering.
   * @param source the data source identifier (e.g., "wallapop", "pisos")
   * @param datasetName the name of the dataset
   * @param spark implicit SparkSession
   * @return DataFrame containing the data
   * @throws Exception if data cannot be read
   */
  def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame
  
  /**
   * Reads sanitized data from a source for a specific date.
   * @param source the data source identifier
   * @param datasetName the name of the dataset
   * @param date the date for which to read data
   * @param spark implicit SparkSession
   * @return DataFrame containing the data
   * @throws Exception if data cannot be read
   */
  def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame
  
  /**
   * Attempts to read sanitized data, returning None if data doesn't exist.
   * @param source the data source identifier
   * @param datasetName the name of the dataset
   * @param date the date for which to read data
   * @param spark implicit SparkSession
   * @return Some(DataFrame) if data exists, None otherwise
   */
  def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame]
  
  /**
   * Reads processed data from a dataset.
   * @param dataset the processed table to read
   * @param dateOption optional date filter
   * @param spark implicit SparkSession
   * @return DataFrame containing the data
   * @throws Exception if data cannot be read
   */
  def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame
  
  /**
   * Attempts to read processed data, returning None if data doesn't exist.
   * @param dataset the processed table to read
   * @param dateOption optional date filter
   * @param spark implicit SparkSession
   * @return Some(DataFrame) if data exists, None otherwise
   */
  def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame]
}
