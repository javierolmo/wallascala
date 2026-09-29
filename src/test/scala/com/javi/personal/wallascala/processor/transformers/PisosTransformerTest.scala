package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class PisosTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PisosTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "map operation from url and type from propertyType" in {
    val pisosInput = Seq(
      (
        "p-1",
        "Piso céntrico",
        150000,
        "https://www.pisos.com/comprar/piso-vigo-1/",
        "Buen piso",
        3,
        1,
        80,
        1,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.23,
        -8.72,
        "Piso",
        "Vigo"
      ),
      (
        "p-2",
        "Casa con jardín",
        250000,
        "https://www.pisos.com/alquilar/casa-baiona-2/",
        "Buena casa",
        4,
        2,
        200,
        0,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.11,
        -8.84,
        "Casa",
        "Baiona"
      ),
      (
        "p-3",
        "Local comercial",
        300000,
        "https://www.pisos.com/comprar/local-vigo-3/",
        "Buen local",
        0,
        1,
        120,
        0,
        "http://img.jpg",
        Date.valueOf("2026-09-27"),
        42.24,
        -8.70,
        "Local",
        "Vigo"
      )
    ).toDF(
      "id",
      "title",
      "price",
      "url",
      "fullDescription",
      "rooms",
      "bathrooms",
      "surface",
      "floor",
      "imageUrl",
      "lastUpdateDate",
      "latitude",
      "longitude",
      "propertyType",
      "location"
    )

    val zipCodes = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      new org.apache.spark.sql.types.StructType()
        .add("coordinates", org.apache.spark.sql.types.ArrayType(
          new org.apache.spark.sql.types.StructType()
            .add("latitude", org.apache.spark.sql.types.DoubleType)
            .add("longitude", org.apache.spark.sql.types.DoubleType)
        ))
        .add("codigo_postal", org.apache.spark.sql.types.IntegerType)
        .add("nombre", org.apache.spark.sql.types.StringType)
        .add("provincia", org.apache.spark.sql.types.StringType)
    )

    val result = PisosTransformer.transform(pisosInput, zipCodes)

    result.count() shouldEqual 3

    val rows = result.collect().map(r => (r.getAs[String]("id"), (r.getAs[String]("operation"), r.getAs[String]("type")))).toMap

    rows("p-1") shouldEqual ("Sell", "Flat")
    rows("p-2") shouldEqual ("Rent", "House")
    rows("p-3") shouldEqual ("Sell", "Premises / Office")
  }
}
