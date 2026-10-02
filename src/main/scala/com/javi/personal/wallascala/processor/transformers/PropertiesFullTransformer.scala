package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class PropertiesFullSources(
  wallapopProperties: DataFrame,
  pisosProperties: DataFrame,
  fotocasaProperties: DataFrame
)

object PropertiesFullSources {
  def apply(wallapopProperties: DataFrame, pisosProperties: DataFrame): PropertiesFullSources =
    PropertiesFullSources(wallapopProperties, pisosProperties, wallapopProperties.sparkSession.emptyDataFrame)
}

object PropertiesFullTransformer extends Transformer[PropertiesFullSources, DataFrame] {

  def withLoadDate(df: DataFrame): DataFrame =
    if (df.columns.isEmpty) df
    else {
      val withLd = if (df.columns.contains("year") && df.columns.contains("month") && df.columns.contains("day")) {
        df.withColumn("load_date", make_date(col("year").cast(IntegerType), col("month").cast(IntegerType), col("day").cast(IntegerType)))
      } else {
        df
      }
      val withMod = if (withLd.columns.contains("modification_date") && withLd.columns.contains("load_date")) {
        withLd.withColumn("modification_date", coalesce(to_date(col("modification_date")), col("load_date")))
      } else withLd
      val withCre = if (withMod.columns.contains("creation_date") && withMod.columns.contains("load_date")) {
        withMod.withColumn("creation_date", coalesce(to_date(col("creation_date")), col("load_date")))
      } else withMod

      withCre
    }

  def deduplicateLatest(df: DataFrame): DataFrame = {
    if (df.columns.isEmpty) {
      df
    } else {
      withLoadDate(df)
        .withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy(col("modification_date").desc, col("load_date").desc)))
        .filter(col("row_number") === 1)
        .drop("row_number")
    }
  }

  override def transform(sources: PropertiesFullSources): DataFrame =
    transform(sources.wallapopProperties, sources.pisosProperties, sources.fotocasaProperties)

  def transform(wallapopProperties: DataFrame, pisosProperties: DataFrame): DataFrame = {
    val cleanWallapop = deduplicateLatest(wallapopProperties)
    val cleanPisos = deduplicateLatest(pisosProperties)
    cleanWallapop.unionByName(cleanPisos, allowMissingColumns = true)
  }

  def transform(wallapopProperties: DataFrame, pisosProperties: DataFrame, fotocasaProperties: DataFrame): DataFrame = {
    val cleanWallapop = deduplicateLatest(wallapopProperties)
    val cleanPisos = deduplicateLatest(pisosProperties)
    val cleanFotocasa = deduplicateLatest(fotocasaProperties)

    val base = cleanWallapop.unionByName(cleanPisos, allowMissingColumns = true)
    if (cleanFotocasa.columns.isEmpty) base
    else base.unionByName(cleanFotocasa, allowMissingColumns = true)
  }

}
