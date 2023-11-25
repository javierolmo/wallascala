package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

abstract class SparkWriter(format: String, saveMode: String, options: Map[String, String])(implicit spark: SparkSession) {

  protected def baseWriter(dataFrame: DataFrame): DataFrameWriter[Row] =
    dataFrame.write
      .mode(saveMode)
      .format(format)
      .options(options)


  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit

}
