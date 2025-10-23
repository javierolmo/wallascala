package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.Properties._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions.{col, concat, lit, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.time.format.DateTimeFormatter

@ETL(table = ProcessedTables.PROPERTIES)
class Properties(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

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

  private def emptyDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  private object sources {
    private val date = config.date
    private val dateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    
    lazy val sanitedWallapopProperties: DataFrame = readSanitedOptional("wallapop", "properties", date).map { wallapopProperties =>
      val sanitedProvinces: DataFrame = readSanited("opendatasoft", "provincias-espanolas")
      wallapopProperties
        .withColumn("province_code", (col("location__postal_code").cast(IntegerType)/1000).cast(IntegerType))
        .join(sanitedProvinces.as("p"), col("province_code") === sanitedProvinces("codigo").cast(IntegerType), "left")
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
        .withColumn(Date, lit(dateStr))
        .dropDuplicates(Title, Price, Description, Surface, Operation)
        .select(schema.fields.map(field => col(field.name).cast(field.dataType)):_*)
    }.getOrElse(emptyDataFrame)

    lazy val sanitedPisosProperties: DataFrame = readSanitedOptional("pisos", "properties", date).map { pisosProperties =>
      pisosProperties
        .withColumn(Surface, col("size"))
        .withColumn(Bathrooms, col("bathrooms"))
        .withColumn(Link, concat(lit("https://www.pisos.com"), col("url")))
        .withColumn(Source, lit("pisos"))
        .withColumn(CreationDate, lit(null)) // TODO: get creation date from fotocasa
        .withColumn(Currency, lit("EUR"))
        .withColumn(Elevator, lit(null))
        .withColumn(Garage, lit(null))
        .withColumn(Garden, lit(null))
        .withColumn(Country, lit("ES"))
        .withColumn(PostalCode, lit(null))
        .withColumn(Province, lit(null))
        .withColumn(Region, lit(null))
        .withColumn(ModificationDate, lit(null))
        .withColumn(Pool, lit(null))
        .withColumn(Terrace, lit(null))
        .withColumn(Date, lit(dateStr))
        .dropDuplicates(Title, Price, Description, Surface, Operation)
        .select(schema.fields.map(field => col(field.name).cast(field.dataType)):_*)
    }.getOrElse(emptyDataFrame)

    lazy val sanitedFotocasaProperties: DataFrame = readSanitedOptional("fotocasa", "properties", date).map { fotocasaProperties =>
      fotocasaProperties
        .withColumn(Surface, col("features__size"))
        .withColumn(Rooms, col("features__rooms"))
        .withColumn(Bathrooms, col("features__bathrooms"))
        .withColumn(Link, concat(lit("https://www.fotocasa.es"), col("url")))
        .withColumn(Source, lit("fotocasa"))
        .withColumn(CreationDate, lit(null)) // TODO: get creation date from fotocasa
        .withColumn(Currency, lit("EUR"))
        .withColumn(Elevator, lit(null))
        .withColumn(Garage, lit(null))
        .withColumn(Garden, lit(null))
        .withColumn(Country, lit("ES"))
        .withColumn(PostalCode, lit(null))
        .withColumn(Province, lit(null))
        .withColumn(Region, lit(null))
        .withColumn(ModificationDate, lit(null))
        .withColumn(Pool, lit(null))
        .withColumn(Description, lit(null))
        .withColumn(Terrace, lit(null))
        .withColumn(Date, lit(dateStr))
        .dropDuplicates(Title, Price, Description, Surface, Operation)
        .select(schema.fields.map(field => col(field.name).cast(field.dataType)):_*)
    }.getOrElse(emptyDataFrame)
  }

  override protected def build(): DataFrame = {
    sources.sanitedWallapopProperties
      .union(sources.sanitedPisosProperties)
      .union(sources.sanitedFotocasaProperties)
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
