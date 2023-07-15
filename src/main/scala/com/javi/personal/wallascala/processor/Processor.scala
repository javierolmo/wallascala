package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Processor {

  val spark: SparkSession = SparkSessionFactory.build()

  def apply(datasetName: String): Processor =
    datasetName match {
      case "properties" => new PropertiesProcessor(spark)
    }

}

abstract class Processor(spark: SparkSession) {

  private val log = LogManager.getLogger(getClass)
  protected val datasetName: String
  protected val finalColumns: Array[String]

  protected def readSanited(source: String, datasetName: String): DataFrame = {
    val location = PathBuilder.buildSanitedPath(source, datasetName)
    spark.read
      .format("parquet")
      .load(location.url)
  }

  protected def readProcessed(datasetName: String): DataFrame = {
    val location = PathBuilder.buildProcessedPath(datasetName)
    spark.read
      .format("parquet")
      .load(location.url)
  }

  private def write(dataFrame: DataFrame): Unit =
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(PathBuilder.buildProcessedPath(datasetName).url)


  protected def build(): DataFrame

  final def execute(): Unit = {
    val dataFrame = build()
    write(dataFrame.toDF())
  }


}
