package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.Processor
import com.javi.personal.wallascala.processor.tables.Properties._
import org.apache.spark.sql.functions.{col, concat, lit, lpad}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class Properties(dateOption: Option[LocalDate] = Option.empty)(implicit spark: SparkSession) extends Processor(spark) {

  override protected val datasetName: String = "properties"
  override protected val finalColumns: Array[String] = Array(
    Id, Title, Price, Surface, Rooms, Bathrooms, Link, Source, CreationDate, Currency, Elevator, Garage, Garden, City,
    Country, PostalCode, ModificationDate, Operation, Pool, Description, Terrace, Type, ExtractedDate, Year, Month, Day
  )

  // Sources
  private val sanitedWallapopProperties: DataFrame = dateOption match {
    case Some(date) => readSanited("wallapop", "properties").filter(ymdCondition(date))
    case None => readSanited("wallapop", "properties")
  }

  override protected def build(): DataFrame = {
    sanitedWallapopProperties
      .withColumn(City, col("location__city"))
      .withColumn(Country, col("location__country_code"))
      .withColumn(PostalCode, col("location__postal_code"))
      .withColumn(Description, col("storytelling"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(ExtractedDate, col("date"))
      .withColumn(Year, lpad(col(Year), 4, "0"))
      .withColumn(Month, lpad(col(Month), 2, "0"))
      .withColumn(Day, lpad(col(Day), 2, "0"))
      .orderBy(Id)
      .dropDuplicates(Title, Price, Description, Surface, Operation, Year, Month, Day)
  }
}

object Properties {
  val Id = "id"
  val Title = "title"
  val Price = "price"
  val Surface = "surface"
  val Rooms = "rooms"
  val Bathrooms = "bathrooms"
  val Link = "link"
  val Source = "source"
  val CreationDate = "creation_date"
  val Currency = "currency"
  val Elevator = "elevator"
  val Garage =  "garage"
  val Garden = "garden"
  val City = "city"
  val Country = "country"
  val PostalCode = "postal_code"
  val ModificationDate = "modification_date"
  val Operation = "operation"
  val Pool = "pool"
  val Description = "description"
  val Terrace = "terrace"
  val Type = "type"
  val ExtractedDate = "extracted_date"
  val Year = "year"
  val Month = "month"
  val Day = "day"

}
