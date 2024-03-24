package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.tables.Properties._
import com.javi.personal.wallascala.processor.{ProcessedTables, Processor}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class Properties(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: ProcessedTables = ProcessedTables.PROPERTIES
  override protected val schema: StructType = StructType(Array(
      StructField(Id, StringType),
      StructField(Title, StringType),
      StructField(Price, IntegerType),
      StructField(Surface, IntegerType),
      StructField(Rooms, IntegerType),
      StructField(Bathrooms, IntegerType),
      StructField(Link, StringType),
      StructField(Source, StringType),
      StructField(CreationDate, DateType),
      StructField(Currency, StringType),
      StructField(Elevator, BooleanType),
      StructField(Garage, BooleanType),
      StructField(Garden, BooleanType),
      StructField(City, StringType),
      StructField(Country, StringType),
      StructField(PostalCode, IntegerType),
      StructField(Province, StringType),
      StructField(Region, StringType),
      StructField(ModificationDate, DateType),
      StructField(Operation, StringType),
      StructField(Pool, BooleanType),
      StructField(Description, StringType),
      StructField(Terrace, BooleanType),
      StructField(Type, StringType),
      StructField(Date, DateType)
    )
  )

  private object sources {
    val processedWallapopProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES.getName, date)
    val processedFotocasaProperties: DataFrame = readProcessed(ProcessedTables.FOTOCASA_PROPERTIES.getName, date)
    val processedPisosProperties: DataFrame = readProcessed(ProcessedTables.PISOS_PROPERTIES.getName, date)
  }

  override protected def build(): DataFrame = {
    sources.processedFotocasaProperties
      .union(sources.processedWallapopProperties)
      .union(sources.processedPisosProperties)
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
  val Date = "date"
}
