package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.Processor
import com.javi.personal.wallascala.processor.tables.PostalCodeAnalysis._
import com.javi.personal.wallascala.processor.tables.PriceChanges.{Day, Month, Year}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PostalCodeAnalysis (dateOption: Option[LocalDate] = Option.empty)(implicit spark: SparkSession) extends Processor(spark) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: String = "postal_code_analysis"
  override protected val finalColumns: Array[String] = Array(City, PostalCode, Type, Operation, AveragePrice, AverageSurface, AveragePriceM2, Count, Year, Month, Day)

  private val properties = dateOption match {
    case Some(date) => readProcessed("properties").filter(ymdCondition(date))
    case None => readProcessed("properties")
  }

  override protected def build(): DataFrame = {
    val result = properties
      .withColumn(Properties.Surface, when(col(Properties.Surface) === 0, null).otherwise(col(Properties.Surface)))
      .withColumn(Properties.Price, when(col(Properties.Price) === 0, null).otherwise(col(Properties.Price)))
      .withColumn("price_m2", col(Properties.Price) / col(Properties.Surface))
      .groupBy(Properties.PostalCode, Properties.Type, Properties.Operation, Properties.Year, Properties.Month, Properties.Day)
      .agg(
        first(Properties.City).as(City),
        round(avg(Properties.Price), 2).as(AveragePrice),
        round(avg(Properties.Surface), 2).as(AverageSurface),
        round(avg("price_m2"), 2).as(AveragePriceM2),
        count(Properties.Id).as(Count)
      )
      .withColumn(Year, lpad(col(Properties.Year), 4, "0"))
      .withColumn(Month, lpad(col(Properties.Month), 2, "0"))
      .withColumn(Day, lpad(col(Properties.Day), 2, "0"))

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
