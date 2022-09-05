package com.javi.personal
package wallascala.cleaner

import wallascala.Application.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object Cleaner {

  def clean(usecase: String, datasetName: String) = {

    val rawDF = spark.read
      .format("parquet")
      .load(s"data/${usecase}/raw/${datasetName}")
      .coalesce(1)
      .dropDuplicates("id")

    rawDF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .save(s"data/${usecase}/sanited/${datasetName}")
  }
}
