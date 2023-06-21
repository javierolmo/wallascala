package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Processor {

  val spark: SparkSession = SparkSessionFactory.build()

  def execute(datasetName: String): Unit = {
    val processor: Processor = datasetName match {
      case "properties" => new PropertiesProcessor(spark)
    }

    val dataframe = processor.build()
    processor.write(dataframe)
  }

}

abstract class Processor(spark: SparkSession) {

  private val log = LogManager.getLogger(getClass)
  protected val datasetName: String
  protected val finalColumns: Array[String]

  private def write(dataFrame: DataFrame): Unit =
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(PathBuilder.buildProcessedPath(datasetName).url)


  protected def build(): DataFrame

}
