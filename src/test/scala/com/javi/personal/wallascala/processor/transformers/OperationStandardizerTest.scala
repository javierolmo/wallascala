package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OperationStandardizerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("OperationStandardizerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "standardize operation to uppercase SELL or RENT, and convert others to null" in {
    val input = Seq(
      ("1", "sell"),
      ("2", "SELL"),
      ("3", "sale"),
      ("4", "comprar"),
      ("5", "compra"),
      ("6", "venta"),
      ("7", "buy"),
      ("8", "selling"),
      ("9", "rent"),
      ("10", "RENT"),
      ("11", "alquiler"),
      ("12", "alquilar"),
      ("13", "alquilación"),
      ("14", "renting"),
      ("15", "otro_desconocido"),
      ("16", "traspaso")
    ).toDF("id", "raw_operation")

    val result = input.withColumn("std_operation", OperationStandardizer.standardize($"raw_operation"))
    val map = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("std_operation"))).toMap

    map("1") shouldEqual "SELL"
    map("2") shouldEqual "SELL"
    map("3") shouldEqual "SELL"
    map("4") shouldEqual "SELL"
    map("5") shouldEqual "SELL"
    map("6") shouldEqual "SELL"
    map("7") shouldEqual "SELL"
    map("8") shouldEqual "SELL"

    map("9") shouldEqual "RENT"
    map("10") shouldEqual "RENT"
    map("11") shouldEqual "RENT"
    map("12") shouldEqual "RENT"
    map("13") shouldEqual "RENT"
    map("14") shouldEqual "RENT"

    map("15") shouldEqual null
    map("16") shouldEqual null
  }

  it should "extract SELL or RENT from url" in {
    val input = Seq(
      ("1", "https://www.pisos.com/comprar/piso-vigo-1/"),
      ("2", "https://www.pisos.com/alquilar/casa-baiona-2/"),
      ("3", "https://www.fotocasa.es/es/comprar/vivienda/vigo/1/"),
      ("4", "https://www.fotocasa.es/es/alquiler/vivienda/vigo/2/"),
      ("5", "https://www.pisos.com/venta/local-vigo-3/"),
      ("6", "https://www.pisos.com/sin_operacion/item-4/")
    ).toDF("id", "url")

    val result = input.withColumn("url_operation", OperationStandardizer.fromUrl($"url"))
    val map = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("url_operation"))).toMap

    map("1") shouldEqual "SELL"
    map("2") shouldEqual "RENT"
    map("3") shouldEqual "SELL"
    map("4") shouldEqual "RENT"
    map("5") shouldEqual "SELL"
    map("6") shouldEqual null
  }

}
