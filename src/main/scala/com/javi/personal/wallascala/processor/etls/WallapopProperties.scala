package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.WallapopProperties._
import com.javi.personal.wallascala.processor.transformers.WallapopTransformer
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.writers.SparkWriter
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.WALLAPOP_PROPERTIES)
class WallapopProperties(
  config: ProcessorConfig,
  dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider(),
  customWriter: Option[SparkWriter] = None
)(implicit spark: SparkSession) extends Processor(config, dataSourceProvider, customWriter) {

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
      StructField(Latitude, DoubleType),
      StructField(Longitude, DoubleType)
    )
  )

  private object sources {
    lazy val sanitedWallapopProperties: DataFrame = dataSourceProvider.readSilver("wallapop", "properties", config.date)
    lazy val sanitedProvinces: DataFrame = dataSourceProvider.readSilver("opendatasoft", "provincias-espanolas")
  }

  override protected def build(): DataFrame =
    WallapopTransformer.transform(sources.sanitedWallapopProperties, sources.sanitedProvinces)

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
  val Latitude = "latitude"
  val Longitude = "longitude"
}
