package com.javi.personal.wallascala.model

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("com/javi/personal/wallascala")
    .getOrCreate

}
