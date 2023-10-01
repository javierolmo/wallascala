package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.Processor
import com.javi.personal.wallascala.processor.tables.PostalCodeAnalysis._
import com.javi.personal.wallascala.processor.tables.PriceChanges.{Day, Month, Year}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PostalCodeAnalysis (date: LocalDate = LocalDate.now())(implicit spark: SparkSession) extends Processor(spark) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: String = "postal_code_analysis"
  override protected val finalColumns: Array[String] = Array(City, PostalCode, Type, Operation, AveragePrice, AverageSurface, AveragePriceM2, Count, Year, Month, Day)

  private val properties = readProcessed("properties")

  override protected def build(): DataFrame = {
    val result = properties
      .filter(col(Properties.ExtractedDate).geq(date))
      .withColumn("row_number", row_number().over(Window.partitionBy(Properties.Id).orderBy(col(Properties.ExtractedDate).desc)))
      .filter(col("row_number") === 1)
      .withColumn(Properties.Surface, when(col(Properties.Surface) === 0, null).otherwise(col(Properties.Surface)))
      .withColumn(Properties.Price, when(col(Properties.Price) === 0, null).otherwise(col(Properties.Price)))
      .withColumn("price_m2", col(Properties.Price) / col(Properties.Surface))
      .groupBy(Properties.PostalCode, Properties.Type, Properties.Operation)
      .agg(
        first(Properties.City).as(City),
        round(avg(Properties.Price), 2).as(AveragePrice),
        round(avg(Properties.Surface), 2).as(AverageSurface),
        round(avg("price_m2"), 2).as(AveragePriceM2),
        count(Properties.Id).as(Count)
      )
      .withColumn(Year, lpad(lit(date.getYear), 4, "0"))
      .withColumn(Month, lpad(lit(date.getMonthValue), 2, "0"))
      .withColumn(Day, lpad(lit(date.getDayOfMonth), 2, "0"))

    result
  }
}

object PostalCodeAnalysis {
  val City = "city"
  val PostalCode = "postal_code"
  val Type = "type"
  val Operation = "operation"
  val AveragePrice = "average_price"
  val AverageSurface = "average_surface"
  val AveragePriceM2 = "average_price_m2"
  val Count = "count"
  val Year = "year"
  val Month = "month"
  val Day = "day"
}