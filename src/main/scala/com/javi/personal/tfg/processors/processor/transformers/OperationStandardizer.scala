package com.javi.personal.tfg.processors.processor.transformers

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object OperationStandardizer {

  val SELL = "SELL"
  val RENT = "RENT"

  def standardize(colExpr: Column): Column = {
    val upperOp = upper(trim(colExpr))
    when(upperOp.isin("SELL", "SALE", "COMPRAR", "COMPRA", "VENTA", "BUY", "SELLING"), SELL)
      .when(upperOp.isin("RENT", "ALQUILER", "ALQUILAR", "ALQUILACIÓN", "ALQUILACION", "RENTING"), RENT)
      .otherwise(lit(null).cast(StringType))
  }

  def fromUrl(urlExpr: Column): Column = {
    val upperUrl = upper(urlExpr)
    when(upperUrl.contains("/COMPRAR/") || upperUrl.contains("/COMPRA/") || upperUrl.contains("/VENTA/") || upperUrl.contains("/SELL/"), SELL)
      .when(upperUrl.contains("/ALQUILAR/") || upperUrl.contains("/ALQUILER/") || upperUrl.contains("/RENT/"), RENT)
      .otherwise(lit(null).cast(StringType))
  }

}
