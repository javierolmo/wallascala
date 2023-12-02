package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

/**
 * SparkWriter is an abstract class that defines the basic methods to write a DataFrame.
 * @param format The format of the DataFrame to write.
 * @param saveMode The save mode of the DataFrame to write.
 * @param options The options of the DataFrame to write.
 * @param spark The SparkSession to use.
 */
abstract class SparkWriter(format: String, saveMode: String, options: Map[String, String])(implicit spark: SparkSession) {

  /**
   * This method returns a DataFrameWriter[Row] with the basic configuration.
   * @param dataFrame The DataFrame to write.
   * @return A DataFrameWriter[Row] with the basic configuration.
   */
  protected def baseWriter(dataFrame: DataFrame): DataFrameWriter[Row] =
    dataFrame.write
      .mode(saveMode)
      .format(format)
      .options(options)


  /**
   * This method writes a DataFrame. It is an abstract method that must be implemented by the subclasses.
   * @param dataFrame The DataFrame to write.
   * @param spark The SparkSession to use.
   */
  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit

}
