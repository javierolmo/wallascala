package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.PriceChanges._
import com.javi.personal.wallascala.processor.etls.Properties.{Bathrooms, City, Country, CreationDate, Currency, Date, Description, Elevator, Garage, Garden, Id, Link, ModificationDate, Operation, Pool, PostalCode, Price, Province, Region, Rooms, Source, Surface, Terrace, Title, Type}
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor}
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

@ETL(table = ProcessedTables.PRICE_CHANGES)
case class PriceChanges(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val writerCoalesce: Option[Int] = Some(1)
  override protected val schema: StructType = StructType(Array(
    StructField(Id, StringType),
    StructField(PreviousPrice, IntegerType),
    StructField(NewPrice, IntegerType),
    StructField(Discount, DoubleType)
  ))

  object sources {
    val todayProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(date))
      .select(Properties.Id, Properties.Price).as("tp")
    val yesterdayProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(date.minusDays(1)))
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
