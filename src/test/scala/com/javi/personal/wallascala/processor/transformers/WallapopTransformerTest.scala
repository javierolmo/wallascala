package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

}
