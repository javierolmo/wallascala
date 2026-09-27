package com.javi.personal.wallascala.processor.transformers

import com.javi.personal.wallascala.processor.etls.FotocasaProperties._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

case class FotocasaSources(sanitedFotocasa: DataFrame, zipCodes: DataFrame)

object FotocasaSources {
  def apply(sanitedFotocasa: DataFrame): FotocasaSources = FotocasaSources(sanitedFotocasa, null)
}

object FotocasaTransformer extends Transformer[FotocasaSources, DataFrame] {

  val pointInPolygon = udf((lat: java.lang.Double, lon: java.lang.Double, coordinates: Seq[Map[String, Double]]) => {
    if (lat == null || lon == null || coordinates == null || coordinates.length < 3) {
      false
    } else {
      try {
        val coords: Array[Coordinate] = coordinates.map(coord => new Coordinate(coord("longitude"), coord("latitude"))).toArray
        val closedCoords = if (coords.nonEmpty && coords.head.equals2D(coords.last)) coords else coords :+ coords.head
        val gf = new GeometryFactory()
        val polygon = gf.createPolygon(closedCoords)
        val point = gf.createPoint(new Coordinate(lon, lat))
        polygon.contains(point)
      } catch {
        case _: Throwable => false
      }
    }
  })

  override def transform(sources: FotocasaSources): DataFrame =
    transform(sources.sanitedFotocasa, sources.zipCodes)

  def transform(sanitedFotocasa: DataFrame): DataFrame =
    transform(sanitedFotocasa, null)

  def transform(sanitedFotocasa: DataFrame, zipCodes: DataFrame): DataFrame = {
    val mappedOperation = when(lower(col("operacion")).isin("comprar", "sell", "compra", "venta"), "Sell")
      .when(lower(col("operacion")).isin("alquiler", "rent", "alquilar"), "Rent")
      .otherwise(col("operacion"))

    val rawType = lower(coalesce(col("tipo_detalle"), col("tipo_inmueble")))
    val mappedType = when(rawType.isin("flat", "viviendas", "duplex", "dúplex", "penthouse", "ático", "atico", "studio", "estudio", "loft", "apartamento"), "Flat")
      .when(rawType.isin("house", "countryhouse", "casas", "chalet", "adossat", "pareado"), "House")
      .when(rawType.isin("business", "building", "locales", "edificios", "oficinas", "premises / office"), "Premises / Office")
      .when(rawType.isin("garage", "garajes", "parking"), "Garage")
      .when(rawType.isin("storage", "trasteros", "box room"), "Box Room")
      .when(rawType.isin("land", "terrenos", "parcelas", "suelo"), "Land")
      .when(rawType.isin("room", "habitaciones"), "Room")
      .otherwise(coalesce(col("tipo_detalle"), col("tipo_inmueble")))

    val base = sanitedFotocasa
      .withColumn(Id, col("id").cast(StringType))
      .withColumn(Title, lit(null).cast(StringType))
      .withColumn(Price, col("precio").cast(IntegerType))
      .withColumn(Surface, col("metros").cast(IntegerType))
      .withColumn(Rooms, col("habitaciones").cast(IntegerType))
      .withColumn(Bathrooms, col("baños").cast(IntegerType))
      .withColumn(Link, col("url"))
      .withColumn(Source, lit("fotocasa"))
      .withColumn(CreationDate, lit(null).cast(DateType))
      .withColumn(Elevator, lit(null).cast(BooleanType))
      .withColumn(Garage, lit(null).cast(BooleanType))
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(City, col("municipio"))
      .withColumn(Country, lit("ES"))
      .withColumn(Province, initcap(regexp_replace(regexp_replace(col("provincia"), "-provincia$", ""), "-", " ")))
      .withColumn(Region, lit(null).cast(StringType))
      .withColumn(ModificationDate, to_date(col("fecha_scraping")))
      .withColumn(Operation, mappedOperation)
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Description, lit(null).cast(StringType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumn(Type, mappedType)
      .withColumn(Latitude, col("coordenadas__latitude").cast(DoubleType))
      .withColumn(Longitude, col("coordenadas__longitude").cast(DoubleType))

    val withPostalCode = if (zipCodes != null && zipCodes.columns.contains("coordinates") && zipCodes.columns.contains("codigo_postal")) {
      val zipCodesWithPolygon = broadcast(
        zipCodes
          .select("codigo_postal", "coordinates")
          .withColumn("coordinates", expr("transform(coordinates, x -> map_from_arrays(array('latitude', 'longitude'), array(x.latitude, x.longitude)))"))
      )

      base
        .join(zipCodesWithPolygon, pointInPolygon(col(Latitude), col(Longitude), col("coordinates")) === lit(true), "left")
        .withColumn(PostalCode, col("codigo_postal").cast(IntegerType))
        .drop("codigo_postal", "coordinates")
    } else {
      base.withColumn(PostalCode, lit(null).cast(IntegerType))
    }

    withPostalCode
      .withColumn("row_number", row_number().over(Window.partitionBy(Id).orderBy(col(ModificationDate).desc)))
      .filter(col("row_number") === 1)
      .drop("row_number")
  }

}
