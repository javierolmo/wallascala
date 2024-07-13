package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.WallapopPropertiesSnapshots._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.WALLAPOP_PROPERTIES_SNAPSHOTS)
class WallapopPropertiesSnapshots(config: ProcessorConfig)(implicit spark: SparkSession) extends Processor(config) {

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
    lazy val wallapopProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Option.empty)
  }

  override protected def build(): DataFrame = {
    val result = sources.wallapopProperties
      .withColumn(StartDate, min(col(WallapopProperties.Date)).over(Window.partitionBy(Id)))
      .withColumn(EndDate, max(col(WallapopProperties.Date)).over(Window.partitionBy(Id)))
      .withColumn(AbsoluteMaxDate, max(col(WallapopProperties.Date)).over())
      .withColumn(EndDate, when(col(EndDate) === col(AbsoluteMaxDate), lit(null)).otherwise(col(EndDate)))
      .withColumn(RowNumber, row_number().over(Window.partitionBy(Id).orderBy(col(WallapopProperties.Date).desc)))
      .filter(col(RowNumber) === 1)

    result
  }

}

object WallapopPropertiesSnapshots {
  private val AbsoluteMaxDate = "absolute_max_date"
  private val RowNumber = "row_number"

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
