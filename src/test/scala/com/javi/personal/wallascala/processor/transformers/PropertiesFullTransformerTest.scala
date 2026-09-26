package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class PropertiesFullTransformerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PropertiesFullTransformerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "deduplicate latest record by modification_date for each dataset and union them" in {
    val wallapopDf = Seq(
      ("w-1", "Walla Antiguo", Date.valueOf("2024-01-01")),
      ("w-1", "Walla Nuevo", Date.valueOf("2024-01-05"))
    ).toDF("id", "title", "modification_date")

    val pisosDf = Seq(
      ("p-1", "Pisos Antiguo", Date.valueOf("2024-01-02")),
      ("p-1", "Pisos Nuevo", Date.valueOf("2024-01-10")),
      ("p-2", "Pisos Unico", Date.valueOf("2024-01-03"))
    ).toDF("id", "title", "modification_date")

    val result = PropertiesFullTransformer.transform(wallapopDf, pisosDf)

    result.count() shouldEqual 3

    val rows = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("title"))).toMap
    rows("w-1") shouldEqual "Walla Nuevo"
    rows("p-1") shouldEqual "Pisos Nuevo"
    rows("p-2") shouldEqual "Pisos Unico"
  }

}
