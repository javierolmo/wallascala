package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.tables.PriceChanges._
import com.javi.personal.wallascala.processor.{ProcessedTables, Processor}
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PriceChanges(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: ProcessedTables = ProcessedTables.PRICE_CHANGES
  override protected val finalColumns: Array[String] = Array(
    Id, PreviousPrice, NewPrice, Discount
  )

  object sources {
    val todayProperties: DataFrame = readProcessed("properties", date)
      .select(Properties.Id, Properties.Price).as("tp")
    val yesterdayProperties: DataFrame = readProcessed("properties", date.minusDays(1))
      .select(Properties.Id, Properties.Price).as("yp")
  }

  override protected def build(): DataFrame = {
    val result = sources.todayProperties
      .join(sources.yesterdayProperties, Id)
      .filter(sources.yesterdayProperties(Properties.Price) =!= sources.todayProperties(Properties.Price))
      .withColumn(PreviousPrice, sources.yesterdayProperties(Properties.Price))
      .withColumn(NewPrice, sources.todayProperties(Properties.Price))
      .withColumn(DiscountRate, round((col(NewPrice) - col(PreviousPrice)) / col(PreviousPrice), 4))
      .withColumn(Discount, col(NewPrice) - col(PreviousPrice))
    result
  }

}

object PriceChanges {
  val Id = "id"
  val PreviousPrice = "previous_price"
  val NewPrice = "new_price"
  val Discount = "discount"
  val DiscountRate = "discount_rate"
  val Year = "year"
  val Month = "month"
  val Day = "day"
}
