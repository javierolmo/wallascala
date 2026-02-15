package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.WallapopPropertiesSnapshots._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.WALLAPOP_PROPERTIES_SNAPSHOTS)
class WallapopPropertiesSnapshots(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

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
      StructField(Type, StringType)
  ))

  private object sources {
    def wallapopProperties: DataFrame = dataSourceProvider.readGold(ProcessedTables.WALLAPOP_PROPERTIES)
  }

  override protected def build(): DataFrame = {
    val provinciasData = dataSourceProvider.readSilver("opendatasoft", "provincias-espanolas")

    sources.wallapopProperties
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType) / 1000).cast(IntegerType))
      .join(provinciasData.as("p"), col("province_code") === provinciasData("codigo").cast(IntegerType), "left")
      .withColumnRenamed("id", Id)
      .withColumnRenamed("title", Title)
      .withColumnRenamed("price__amount", Price)
      .withColumnRenamed("type_attributes__surface", Surface)
      .withColumnRenamed("type_attributes__rooms", Rooms)
      .withColumnRenamed("type_attributes__bathrooms", Bathrooms)
      .withColumnRenamed("price__currency", Currency)
      .withColumnRenamed("location__city", City)
      .withColumnRenamed("location__country_code", Country)
      .withColumnRenamed("location__postal_code", PostalCode)
      .withColumnRenamed("location__region", Region)
      .withColumnRenamed("provincia", Province)
      .withColumnRenamed("type_attributes__operation", Operation)
      .withColumnRenamed("type_attributes__type", Type)
      .withColumnRenamed("description", Description)
      .withColumnRenamed("modified_at", ModificationDate)
      .withColumn(Source, lit("wallapop"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, to_date(col(ModificationDate)))
      .withColumn(Elevator, lit(null).cast(BooleanType))
      .withColumn(Garage, lit(null).cast(BooleanType))
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumn(RowNumber, row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc, col(CreationDate).desc)))
      .filter(col(RowNumber) === 1)
  }

}

object WallapopPropertiesSnapshots {
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
}
