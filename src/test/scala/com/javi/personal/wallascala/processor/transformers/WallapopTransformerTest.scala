package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class WallapopTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("WallapopTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "transform wallapop data, enrich with provinces and generate link correctly" in {
    val wallapopInput = Seq(
      (
        "item-1",
        "Piso bonito",
        150000,
        80,
        3,
        1,
        "Madrid",
        "ES",
        28001,
        "Comunidad de Madrid",
        "sale",
        "flat",
        "Buen estado",
        "2024-01-02",
        "piso-bonito-1",
        "2024-01-01",
        40.4168,
        -3.7038
      )
    ).toDF(
      "id",
      "title",
      "price__amount",
      "type_attributes__surface",
      "type_attributes__rooms",
      "type_attributes__bathrooms",
      "location__city",
      "location__country_code",
      "location__postal_code",
      "location__region",
      "type_attributes__operation",
      "type_attributes__type",
      "description",
      "modified_at",
      "web_slug",
      "created_at",
      "location__latitude",
      "location__longitude"
    )

    val provincesInput = Seq(
      (28, "Madrid")
    ).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)

    result.count() shouldEqual 1

    val row = result.first()
    row.getAs[String]("id") shouldEqual "item-1"
    row.getAs[String]("title") shouldEqual "Piso bonito"
    row.getAs[Int]("price") shouldEqual 150000
    row.getAs[String]("province") shouldEqual "Madrid"
    row.getAs[String]("source") shouldEqual "wallapop"
    row.getAs[String]("link") shouldEqual "https://es.wallapop.com/item/piso-bonito-1"
    row.getAs[String]("type") shouldEqual "FLAT"
    row.getAs[String]("operation") shouldEqual "SELL"
  }

  it should "standardize wallapop types such as Premises / Office and Box Room" in {
    val wallapopInput = Seq(
      ("w-1", "Local", 100000, 50, 0, 1, "Madrid", "ES", 28001, "Madrid", "Sell", "Premises / Office", "Desc", "2024-01-01", "slug-1", "2024-01-01", 40.0, -3.0),
      ("w-2", "Trastero", 10000, 10, 0, 0, "Madrid", "ES", 28001, "Madrid", "Rent", "Box Room", "Desc", "2024-01-01", "slug-2", "2024-01-01", 40.0, -3.0),
      ("w-3", "Chalet", 300000, 200, 4, 2, "Madrid", "ES", 28001, "Madrid", "sale", "House", "Desc", "2024-01-01", "slug-3", "2024-01-01", 40.0, -3.0)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude"
    )

    val provincesInput = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    val map = result.collect().map(r => (r.getAs[String]("id"), (r.getAs[String]("operation"), r.getAs[String]("type")))).toMap

    map("w-1") shouldEqual ("SELL", "OFFICE")
    map("w-2") shouldEqual ("RENT", "BOXROOM")
    map("w-3") shouldEqual ("SELL", "HOUSE")
  }

  it should "deduplicate records matching Title, Price, Description, Surface, Operation" in {
    val wallapopInput = Seq(
      ("item-1", "Duplicado", 100000, 50, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-01", "slug-1", "2024-01-01", 40.0, -3.0),
      ("item-2", "Duplicado", 100000, 50, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-02", "slug-2", "2024-01-02", 40.0, -3.0)
    ).toDF(
      "id",
      "title",
      "price__amount",
      "type_attributes__surface",
      "type_attributes__rooms",
      "type_attributes__bathrooms",
      "location__city",
      "location__country_code",
      "location__postal_code",
      "location__region",
      "type_attributes__operation",
      "type_attributes__type",
      "description",
      "modified_at",
      "web_slug",
      "created_at",
      "location__latitude",
      "location__longitude"
    )

    val provincesInput = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    result.count() shouldEqual 1
  }

  it should "transform using WallapopSources case class input" in {
    val wallapopInput = Seq(
      ("item-1", "Title", 50000, 40, 1, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-01", "slug", "2024-01-01", 40.0, -3.0)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude"
    )
    val provincesInput = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val result = WallapopTransformer.transform(WallapopSources(wallapopInput, provincesInput))
    result.count() shouldEqual 1
    result.first().getAs[String]("id") shouldEqual "item-1"
  }

  it should "enrich Region from ccaa, Province from opendatasoft, and City from zipCodes" in {
    val wallapopInput = Seq(
      ("w-loc-1", "Piso en Vigo", 120000, 75, 2, 1, "Vigo Raw", "ES", 36201, "Galicia Raw", "sale", "flat", "Desc", "2024-01-02", "slug-loc", "2024-01-01", 42.23, -8.72)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude"
    )

    val provincesInput = Seq((36, "Pontevedra", "Galicia")).toDF("codigo", "provincia", "ccaa")
    val zipCodesInput = Seq((36201, "Vigo Standard")).toDF("codigo_postal", "nombre")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput, zipCodesInput)
    val row = result.first()
    row.getAs[String]("province") shouldEqual "Pontevedra"
    row.getAs[String]("region") shouldEqual "Galicia"
    row.getAs[String]("city") shouldEqual "Vigo Standard"
  }

  it should "fallback creation_date and modification_date to scrap date from year, month, day when null" in {
    val wallapopInput = Seq(
      ("w-date-1", "Piso", 100000, 60, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", null, "slug-date", null, 40.0, -3.0, 2026, 9, 30)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude",
      "year", "month", "day"
    )

    val provincesInput = Seq((28, "Madrid", "Comunidad de Madrid")).toDF("codigo", "provincia", "ccaa")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    val row = result.first()
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-09-30")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-09-30")
  }

  it should "recover corrupted epoch milliseconds timestamps with year > 2050" in {
    val wallapopInput = Seq(
      ("w-corrupt-1", "Piso", 100000, 60, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "+58011-09-30 02:07:08", "slug-corrupt", "+58011-09-29 23:12:36", 40.0, -3.0, 2026, 9, 30)
    ).toDF(
      "id", "title", "price__amount", "type_attributes__surface", "type_attributes__rooms", "type_attributes__bathrooms",
      "location__city", "location__country_code", "location__postal_code", "location__region", "type_attributes__operation",
      "type_attributes__type", "description", "modified_at", "web_slug", "created_at", "location__latitude", "location__longitude",
      "year", "month", "day"
    )

    val provincesInput = Seq((28, "Madrid", "Comunidad de Madrid")).toDF("codigo", "provincia", "ccaa")

    val result = WallapopTransformer.transform(wallapopInput, provincesInput)
    val row = result.first()
    row.getAs[Date]("creation_date") shouldEqual Date.valueOf("2026-01-15")
    row.getAs[Date]("modification_date") shouldEqual Date.valueOf("2026-01-15")
  }

}
