package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionFactory.build()
    val config = CleanerConfig.parse(args)
    Cleaner.execute(config)
    spark.stop()
  }

}
