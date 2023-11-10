package com.javi.personal.wallascala.utils.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {

  def read()(implicit spark: SparkSession): DataFrame

}
