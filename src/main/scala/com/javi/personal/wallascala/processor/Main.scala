package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionFactory.build()
    val config = ProcessorConfig.parse(args)
    Processor.build(config).execute()
    spark.stop()
  }

}
