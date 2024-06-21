package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.WallapopProperties._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor}
import org.apache.spark.sql.functions.{col, concat, lit, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

@ETL(table = ProcessedTables.WALLAPOP_PROPERTIES)
class WallapopProperties(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val writerCoalesce: Option[Int] = Some(1)
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
    val sanitedWallapopProperties: DataFrame = readSanited("wallapop", "properties", date)
    val sanitedProvinces: DataFrame = readSanited("opendatasoft", "provincias-espanolas")
  }

  override protected def build(): DataFrame = {
    sources.sanitedWallapopProperties
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType)/1000).cast(IntegerType))
      .join(sources.sanitedProvinces.as("p"), col("province_code") === sources.sanitedProvinces("codigo").cast(IntegerType), "left")
      .withColumnRenamed("location__city", City)
      .withColumnRenamed("location__country_code", Country)
      .withColumnRenamed("location__postal_code", PostalCode)
      .withColumnRenamed("provincia", Province)
      .withColumnRenamed("ccaa", Region)
      .withColumnRenamed("storytelling", Description)
      .withColumn(Source, lit("wallapop"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, to_date(col(CreationDate)))
      .withColumn(ModificationDate, to_date(col(ModificationDate)))
      .withColumn(Date, lit(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
      .dropDuplicates(Title, Price, Description, Surface, Operation)
      .select(schema.fields.map(field => col(field.name).cast(field.dataType)):_*)
  }

}

object WallapopProperties {
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
