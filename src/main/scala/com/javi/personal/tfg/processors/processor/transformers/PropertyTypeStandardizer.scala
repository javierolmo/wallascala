package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object PropertyTypeStandardizer {

  val FLAT = "FLAT"
  val HOUSE = "HOUSE"
  val OFFICE = "OFFICE"
  val LAND = "LAND"
  val GARAGE = "GARAGE"
  val ROOM = "ROOM"
  val BOXROOM = "BOXROOM"

  def standardize(colExpr: Column): Column = {
    val upperType = upper(trim(colExpr))
    when(upperType.isin(
      "FLAT", "PISO", "ÁTICO", "ATICO", "DÚPLEX", "DUPLEX", "ESTUDIO", "STUDIO", "LOFT", "APARTAMENTO", "VIVIENDAS", "PENTHOUSE"
    ), FLAT)
      .when(upperType.isin(
        "HOUSE", "CASA", "CHALET", "ADOSSAT", "PAREADO", "COUNTRYHOUSE", "CASAS"
      ), HOUSE)
      .when(upperType.isin(
        "OFFICE", "PREMISES / OFFICE", "PREMISES/OFFICE", "LOCAL", "LOCALES", "OFICINA", "OFICINAS", "NAVE", "BUSINESS", "BUILDING", "EDIFICIO", "EDIFICIOS"
      ), OFFICE)
      .when(upperType.isin(
        "GARAGE", "GARAJE", "GARAJES", "PARKING"
      ), GARAGE)
      .when(upperType.isin(
        "BOXROOM", "BOX ROOM", "TRASTERO", "TRASTEROS", "STORAGE"
      ), BOXROOM)
      .when(upperType.isin(
        "LAND", "TERRENO", "TERRENOS", "FINCA", "SUELO", "PARCELA", "PARCELAS"
      ), LAND)
      .when(upperType.isin(
        "ROOM", "HABITACIÓN", "HABITACION", "HABITACIONES"
      ), ROOM)
      .otherwise(lit(null).cast(StringType))
  }

  def fromUrl(urlExpr: Column): Column = {
    // Strip protocol and domain (e.g. https://www.pisos.com) to avoid false positive matching on domain name
    val pathOnly = upper(regexp_replace(urlExpr, "^https?://[^/]+", ""))
    when(pathOnly.contains("APARTAMENTO") || pathOnly.contains("/PISO") || pathOnly.contains("-PISO") || pathOnly.contains("_PISO") ||
      pathOnly.contains("ATICO") || pathOnly.contains("ÁTICO") ||
      pathOnly.contains("DUPLEX") || pathOnly.contains("DÚPLEX") ||
      pathOnly.contains("ESTUDIO") || pathOnly.contains("LOFT"), FLAT)
      .when(pathOnly.contains("CASA") || pathOnly.contains("CHALET") || pathOnly.contains("ADOSSAT") ||
        pathOnly.contains("PAREADO"), HOUSE)
      .when(pathOnly.contains("LOCAL") || pathOnly.contains("OFICINA") || pathOnly.contains("NAVE") ||
        pathOnly.contains("EDIFICIO") || pathOnly.contains("BUSINESS"), OFFICE)
      .when(pathOnly.contains("GARAJE") || pathOnly.contains("PARKING"), GARAGE)
      .when(pathOnly.contains("TRASTERO") || pathOnly.contains("STORAGE"), BOXROOM)
      .when(pathOnly.contains("TERRENO") || pathOnly.contains("PARCELA") || pathOnly.contains("SUELO") ||
        pathOnly.contains("FINCA"), LAND)
      .when(pathOnly.contains("HABITACION") || pathOnly.contains("HABITACIÓN"), ROOM)
      .otherwise(lit(null).cast(StringType))
  }

}
