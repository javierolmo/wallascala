package com.javi.personal.wallascala

import com.javi.personal.wallascala.processor.{DataSourceProvider, DefaultDataSourceProvider, ProcessedTables}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

/**
 * Provides utility methods and implicit conversions for working with Spark DataFrames.
 * Classes mixing in this trait should provide their own DataSourceProvider implementation.
 */
trait SparkUtils {

  /**
   * DataSourceProvider to be overridden by implementing classes.
   * Defaults to DefaultDataSourceProvider for backwards compatibility.
   */
  protected def dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider()

  /**
   * Reads sanitized data from the specified source and dataset.
   */
  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName)

  /**
   * Reads sanitized data from the specified source, dataset, and date.
   */
  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName, date)

  /**
   * Reads sanitized data optionally, returning None if data doesn't exist.
   */
  protected def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readSanitedOptional(source, datasetName, date)

  /**
   * Reads processed data from the specified dataset.
   */
  protected def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readProcessed(dataset, dateOption)

  /**
   * Reads processed data optionally, returning None if data doesn't exist.
   */
  protected def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readProcessedOptional(dataset, dateOption)


  /**
   * Implicit conversion to add utility methods to DataFrame.
   */
  implicit class DataFrameOps(dataFrame: DataFrame) {
    /**
     * Applies a transformation function to the DataFrame only if the condition is true.
     * @param condition whether to apply the transformation
     * @param function the transformation to apply
     * @return the transformed DataFrame if condition is true, otherwise the original DataFrame
     */
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }

}
