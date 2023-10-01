package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.Processor
import com.javi.personal.wallascala.processor.tables.PriceChanges.{Day, Discount, Id, Month, NewPrice, PreviousPrice, Year}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lpad, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PriceChanges(dateOption: Option[LocalDate] = Option.empty)(implicit spark: SparkSession) extends Processor(spark) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: String = "price_changes"
  override protected val finalColumns: Array[String] = Array(
    Id, PreviousPrice, NewPrice, Discount, Year, Month, Day
  )

  private val properties = dateOption match {
    case Some(date) => readProcessed("properties").filter(ymdCondition(date) || ymdCondition(date.minusDays(1)))
    case None => readProcessed("properties")
  }

  override protected def build(): DataFrame = {
    val windowSpec = Window.partitionBy(Properties.Id).orderBy(col(Properties.ExtractedDate).asc)
    properties
      .withColumn(PreviousPrice, lag(Properties.Price, 1).over(windowSpec))
      .withColumn(NewPrice, col(Properties.Price))
      .filter(col(PreviousPrice).isNotNull)
      .filter(col(PreviousPrice) =!= col(NewPrice))
      .withColumn(Discount, round((col(NewPrice) - col(PreviousPrice)) / col(PreviousPrice), 4))
      .withColumn(Year, lpad(col(Year), 4, "0"))
      .withColumn(Month, lpad(col(Month), 2, "0"))
      .withColumn(Day, lpad(col(Day), 2, "0"))
  }
}

object PriceChanges {
  val Id = "id"
  val PreviousPrice = "previous_price"
  val NewPrice = "new_price"
  val Discount = "discount"
  val Year = "year"
  val Month = "month"
  val Day = "day"
}
