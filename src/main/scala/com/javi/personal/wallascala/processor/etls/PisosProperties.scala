package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.PisosProperties._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import java.time.format.DateTimeFormatter

@ETL(table = ProcessedTables.PISOS_PROPERTIES)
class PisosProperties(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

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
      StructField(Longitude, DoubleType),
      StructField(Date, DateType)
    )
  )

  private object sources {
    lazy val sanitedPisosProperties: DataFrame = dataSourceProvider.readSilver("pisos", "properties", config.date)
    lazy val zipCodes: DataFrame = dataSourceProvider.readSilver("cnig", "zip_codes")
  }

  // UDF para comprobar si un punto está dentro del polígono creado a partir de las coordenadas
  private val pointInPolygon = udf((lat: Double, lon: Double, coordinates: Seq[Map[String, Double]]) => {
    val gf = new GeometryFactory()
    val coords: Array[Coordinate] = coordinates.map(coord => new Coordinate(coord("longitude"), coord("latitude"))).toArray
    coords.length match {
      case 0 => false
      case 1 => false
      case 2 => false
      case _ =>
        val polygon = gf.createPolygon(coords)
        val point = gf.createPoint(new Coordinate(lon, lat))
        polygon.contains(point)
    }
  })

  override protected def build(): DataFrame = {
    // Renombrar las columnas de origen antes del cruce
    val pisosRenamed = sources.sanitedPisosProperties
      .withColumnRenamed("id", Id)
      .withColumnRenamed("title", Title)
      .withColumnRenamed("price", Price)
      .withColumnRenamed("surface", Surface)
      .withColumnRenamed("rooms", Rooms)
      .withColumnRenamed("bathrooms", Bathrooms)
      .withColumnRenamed("url", Link)
      .withColumnRenamed("fullDescription", Description)
      .withColumnRenamed("propertyType", Type)
      .withColumnRenamed("latitude", Latitude)
      .withColumnRenamed("longitude", Longitude)
      .withColumnRenamed("lastUpdateDate", ModificationDate)

    // Agrupar las coordenadas por código postal para crear polígonos
    val zipCodesWithPolygon = sources.zipCodes
      .withColumn("coordinates", expr("transform(coordinates, x -> map_from_arrays(array('latitude', 'longitude'), array(x.latitude, x.longitude)))"))

    // Hacer el cruce y filtrar por punto dentro del polígono
    pisosRenamed
      .join(zipCodesWithPolygon, pointInPolygon(col(Latitude), col(Longitude), col("coordinates")) === lit(true), "left")
      .withColumn(Source, lit("pisos.com"))
      .withColumn(CreationDate, lit(null).cast(DateType))
      .withColumn(Elevator, lit(null).cast(BooleanType))
      .withColumn(Garage, lit(null).cast(BooleanType))
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(City, col("nombre"))
      .withColumn(Country, lit("ES"))
      .withColumn(PostalCode, col("codigo_postal").cast(IntegerType))
      .withColumn(Province, col("provincia"))
      .withColumn(Region, lit(null).cast(StringType))
      .withColumn(Operation, lit(null).cast(StringType))
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumn(Date, lit(config.date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc)))
      .filter(col("row_number") === 1)
  }

}

object PisosProperties {
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
  val Date = "date"
}


