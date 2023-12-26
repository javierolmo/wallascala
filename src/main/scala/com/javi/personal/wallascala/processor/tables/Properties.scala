package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.tables.Properties._
import com.javi.personal.wallascala.processor.{ProcessedTables, Processor}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class Properties(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: ProcessedTables = ProcessedTables.PROPERTIES
  override protected val finalColumns: Array[String] = Array(
    Id, Title, Price, Surface, Rooms, Bathrooms, Link, Source, CreationDate, Currency, Elevator, Garage, Garden, City,
    Country, PostalCode, Province, Region, ModificationDate, Operation, Pool, Description, Terrace, Type, ExtractedDate
  )

  object sources {
    val sanitedWallapopProperties: DataFrame = readSanited("wallapop", "properties").filter(ymdCondition(date))
    val sanitedProvinces: DataFrame = readSanited("opendatasoft", "provincias-espanolas")
  }

  override protected def build(): DataFrame = {
    val result = sources.sanitedWallapopProperties
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType)/1000).cast(IntegerType))
      .join(sources.sanitedProvinces.as("p"), col("province_code") === sources.sanitedProvinces("codigo").cast(IntegerType), "left")
      .withColumn(City, col("location__city"))
      .withColumn(Country, col("location__country_code"))
      .withColumn(PostalCode, col("location__postal_code"))
      .withColumn(Province, col("p.provincia"))
      .withColumn(Region, col("p.ccaa"))
      .withColumn(Description, col("storytelling"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, to_date(col(CreationDate)))
      .withColumn(ModificationDate, to_date(col(ModificationDate)))
      .withColumn(ExtractedDate, to_date(col("date")))
      .orderBy(Id)
      .dropDuplicates(Title, Price, Description, Surface, Operation, Year, Month, Day)
    result
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
  val Province = "province"
  val Region = "region"
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
