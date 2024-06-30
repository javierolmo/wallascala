package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.WallapopPropertiesSnapshots._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, min, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

@ETL(table = ProcessedTables.WALLAPOP_PROPERTIES_SNAPSHOTS)
class WallapopPropertiesSnapshots(date: LocalDate)(implicit spark: SparkSession) extends Processor(date) {

  override protected val partitionByDate: Option[LocalDate] = Option.empty
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
      StructField(StartDate, DateType),
      StructField(EndDate, DateType)
    )
  )

  private object sources {
    val wallapopProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Option.empty)
  }

  override protected def build(): DataFrame = {
    val result = sources.wallapopProperties
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(WallapopProperties.Date).desc)))
      .withColumn(StartDate, min(col(WallapopProperties.Date)).over(Window.partitionBy(Id)))
      .withColumn(EndDate, max(col(WallapopProperties.Date)).over(Window.partitionBy(Id)))
      .filter(col("row_number") === 1)

    result
  }

}

object WallapopPropertiesSnapshots {
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
  val StartDate = "start_date"
  val EndDate = "end_date"
}
