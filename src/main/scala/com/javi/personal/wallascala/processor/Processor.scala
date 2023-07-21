package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.processors.{PriceChangesProcessor, PropertiesProcessor}
import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory, SparkUtils}
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object Processor {

  private lazy implicit val spark: SparkSession = SparkSessionFactory.build()

  def properties(): Processor = PropertiesProcessor(Option.empty)
  def properties(date: LocalDate): Processor = PropertiesProcessor(Some(date))
  def priceChanges(): Processor = PriceChangesProcessor(Option.empty)
  def priceChanges(date: LocalDate): Processor = PriceChangesProcessor(Some(date))

}

abstract class Processor(spark: SparkSession) extends SparkUtils {

  protected val datasetName: String
  protected val finalColumns: Array[String]
  protected val coalesce: Option[Int] = Option.empty
  protected def build(): DataFrame

  private def write(dataFrame: DataFrame): Unit =
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .option("path", PathBuilder.buildProcessedPath(datasetName).url)
      .saveAsTable(s"processed.$datasetName")

  final def execute(): Unit = {
    val cols: Array[Column] = finalColumns.map(colName => col(colName))
    val dataFrame = build().select(cols:_*)
    val dataFrameWithCoalesce = if (coalesce.isDefined) dataFrame.coalesce(coalesce.get) else dataFrame
    write(dataFrameWithCoalesce.toDF())
  }


}
