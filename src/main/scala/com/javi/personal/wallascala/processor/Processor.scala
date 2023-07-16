package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object Processor {

  val spark: SparkSession = SparkSessionFactory.build()

  def apply(datasetName: String): Processor =
    datasetName match {
      case "properties" => new PropertiesProcessor(spark)
      case "price_changes_delta" => new PriceChangesDeltaProcessor(spark)
    }

}

abstract class Processor(spark: SparkSession) {

  private val log = LogManager.getLogger(getClass)
  protected val datasetName: String
  protected val finalColumns: Array[String]
  protected val coalesce: Option[Int] = Option.empty

  protected def ymdCondition(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
    col("month") === lit(date.getMonthValue)  &&
    col("day") === lit(date.getDayOfMonth)

  protected def readSanited(source: String, datasetName: String): DataFrame =
    spark.read
      .format("parquet")
      .load(PathBuilder.buildSanitedPath(source, datasetName).url)

  protected def readProcessed(datasetName: String, date: LocalDate): DataFrame =
    readProcessed(datasetName).filter(ymdCondition(date))

  protected def readProcessed(datasetName: String): DataFrame =
    spark.read
      .format("parquet")
      .load(PathBuilder.buildProcessedPath(datasetName).url)

  private def write(dataFrame: DataFrame): Unit =
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .option("path", PathBuilder.buildProcessedPath(datasetName).url)
      .saveAsTable(s"processed.$datasetName")


  protected def build(date: LocalDate): DataFrame

  final def execute(date: LocalDate): Unit = {
    val cols: Array[Column] = finalColumns.map(colName => col(colName))
    val dataFrame = build(date).select(cols:_*)
    val dataFrameWithCoalesce = if (coalesce.isDefined) dataFrame.coalesce(coalesce.get) else dataFrame
    write(dataFrameWithCoalesce.toDF())
  }


}
