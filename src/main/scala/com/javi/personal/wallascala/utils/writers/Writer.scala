package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Writer {

  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit

}
