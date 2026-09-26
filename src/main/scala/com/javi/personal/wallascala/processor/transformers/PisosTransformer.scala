package com.javi.personal.wallascala.processor.transformers

import com.javi.personal.wallascala.processor.etls.PisosProperties._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

case class PisosSources(sanitedPisos: DataFrame, zipCodes: DataFrame)

object PisosTransformer extends Transformer[PisosSources, DataFrame] {

  val pointInPolygon = udf((lat: Double, lon: Double, coordinates: Seq[Map[String, Double]]) => {
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

  override def transform(sources: PisosSources): DataFrame =
    transform(sources.sanitedPisos, sources.zipCodes)

  def transform(sanitedPisos: DataFrame, zipCodes: DataFrame): DataFrame = {
    val pisosRenamed = sanitedPisos
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

    val zipCodesWithPolygon = zipCodes
      .withColumn("coordinates", expr("transform(coordinates, x -> map_from_arrays(array('latitude', 'longitude'), array(x.latitude, x.longitude)))"))

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
      .withColumn(ModificationDate, col("lastUpdateDate"))
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc)))
      .filter(col("row_number") === 1)
  }

}
