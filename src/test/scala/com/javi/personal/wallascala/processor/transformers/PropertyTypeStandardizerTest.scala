package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PropertyTypeStandardizerTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PropertyTypeStandardizerTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "standardize types to uppercase canonical values" in {
    val input = Seq(
      ("1", "flat"),
      ("2", "FLAT"),
      ("3", "piso"),
      ("4", "Piso"),
      ("5", "ático"),
      ("6", "Atico"),
      ("7", "dúplex"),
      ("8", "Duplex"),
      ("9", "estudio"),
      ("10", "Estudio"),
      ("11", "loft"),
      ("12", "Loft"),
      ("13", "apartamento"),
      ("14", "viviendas"),
      ("15", "house"),
      ("16", "casa"),
      ("17", "chalet"),
      ("18", "pareado"),
      ("19", "adossat"),
      ("20", "Premises / Office"),
      ("21", "local"),
      ("22", "oficina"),
      ("23", "nave"),
      ("24", "building"),
      ("25", "edificio"),
      ("26", "garage"),
      ("27", "garaje"),
      ("28", "parking"),
      ("29", "land"),
      ("30", "terreno"),
      ("31", "finca"),
      ("32", "parcela"),
      ("33", "room"),
      ("34", "habitación"),
      ("35", "habitacion"),
      ("36", "box room"),
      ("37", "trastero"),
      ("38", "storage"),
      ("39", "desconocido_xyz")
    ).toDF("id", "raw_type")

    val result = input.withColumn("std_type", PropertyTypeStandardizer.standardize($"raw_type"))
    val map = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("std_type"))).toMap

    map("1") shouldEqual "FLAT"
    map("2") shouldEqual "FLAT"
    map("3") shouldEqual "FLAT"
    map("4") shouldEqual "FLAT"
    map("5") shouldEqual "FLAT"
    map("6") shouldEqual "FLAT"
    map("7") shouldEqual "FLAT"
    map("8") shouldEqual "FLAT"
    map("9") shouldEqual "FLAT"
    map("10") shouldEqual "FLAT"
    map("11") shouldEqual "FLAT"
    map("12") shouldEqual "FLAT"
    map("13") shouldEqual "FLAT"
    map("14") shouldEqual "FLAT"

    map("15") shouldEqual "HOUSE"
    map("16") shouldEqual "HOUSE"
    map("17") shouldEqual "HOUSE"
    map("18") shouldEqual "HOUSE"
    map("19") shouldEqual "HOUSE"

    map("20") shouldEqual "OFFICE"
    map("21") shouldEqual "OFFICE"
    map("22") shouldEqual "OFFICE"
    map("23") shouldEqual "OFFICE"
    map("24") shouldEqual "OFFICE"
    map("25") shouldEqual "OFFICE"

    map("26") shouldEqual "GARAGE"
    map("27") shouldEqual "GARAGE"
    map("28") shouldEqual "GARAGE"

    map("29") shouldEqual "LAND"
    map("30") shouldEqual "LAND"
    map("31") shouldEqual "LAND"
    map("32") shouldEqual "LAND"

    map("33") shouldEqual "ROOM"
    map("34") shouldEqual "ROOM"
    map("35") shouldEqual "ROOM"

    map("36") shouldEqual "BOXROOM"
    map("37") shouldEqual "BOXROOM"
    map("38") shouldEqual "BOXROOM"

    map("39") shouldEqual null
  }

  it should "infer type from url correctly when using fromUrl fallback" in {
    val input = Seq(
      ("1", "https://www.pisos.com/alquilar/apartamento-berbes_peritos36202-5851571020_100500/"),
      ("2", "https://www.pisos.com/comprar/finca_rustica-a_pastoriza_baltar-63345985716_108700/"),
      ("3", "https://www.pisos.com/comprar/local_comercial-arteixo_centro_urbano-60002056093_100500/"),
      ("4", "https://www.pisos.com/comprar/garaje-allariz_centro_urbano-55878550555_106500/"),
      ("5", "https://www.pisos.com/alquilar/chalet-vigo-12345/"),
      ("6", "https://www.pisos.com/alquilar/trastero-madrid-12345/")
    ).toDF("id", "url")

    val result = input.withColumn("url_type", PropertyTypeStandardizer.fromUrl($"url"))
    val map = result.collect().map(r => (r.getAs[String]("id"), r.getAs[String]("url_type"))).toMap

    map("1") shouldEqual "FLAT"
    map("2") shouldEqual "LAND"
    map("3") shouldEqual "OFFICE"
    map("4") shouldEqual "GARAGE"
    map("5") shouldEqual "HOUSE"
    map("6") shouldEqual "BOXROOM"
  }
}
