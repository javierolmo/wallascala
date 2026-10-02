package com.javi.personal.tfg.processors.processor

import com.javi.personal.tfg.processors.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object Main {

  private implicit lazy val spark: SparkSession = SparkSessionFactory.build()

  def main(args: Array[String]): Unit = {
    val config = ProcessorConfig.parse(args)
    Processor.build(config).execute()
  }

}
