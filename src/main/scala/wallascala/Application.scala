package com.javi.personal
package wallascala

import wallascala.extractor.ApiExtractor

import wallascala.cleaner.Cleaner
import org.apache.spark.sql.SparkSession
import wallascala.catalog.DataCatalog

object Application {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("wallascala")
    .getOrCreate

  def main(args: Array[String]): Unit = {

    val datasetName = "iphone"

    ApiExtractor.extractItems(DataCatalog.PLANTA)

    Cleaner.clean("wallapop", datasetName)

  }

}
