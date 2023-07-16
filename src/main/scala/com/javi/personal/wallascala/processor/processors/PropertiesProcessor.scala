package com.javi.personal.wallascala.processor.processors

import com.javi.personal.wallascala.processor.Processor
import org.apache.spark.sql.functions.{col, concat, lit, lpad}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PropertiesProcessor(date: Option[LocalDate])(implicit spark: SparkSession) extends Processor(spark) {

  // Sources
  private val sanitedWallapopProperties: DataFrame =
    if(date.isDefined)
      readSanited("wallapop", "properties").filter(ymdCondition(date.get))
    else
      readSanited("wallapop", "properties")


  override protected val datasetName: String = "properties"
  override protected val finalColumns: Array[String] = Array(
    "id", "title", "price", "surface", "rooms", "bathrooms", "link", "source", "creation_date", "currency",
    "elevator", "garage", "garden", "city", "country", "postal_code", "modification_date", "operation", "pool",
    "description", "terrace", "type", "extracted_date", "year", "month", "day"
  )

  override protected def build(): DataFrame = {
    sanitedWallapopProperties
      .withColumn("city", col("location__city"))
      .withColumn("country", col("location__country_code"))
      .withColumn("postal_code", col("location__postal_code"))
      .withColumn("description", col("storytelling"))
      .withColumn("link", concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn("extracted_date", col("date"))
      .withColumn("year", lpad(col("year"), 4, "0"))
      .withColumn("month", lpad(col("month"), 2, "0"))
      .withColumn("day", lpad(col("day"), 2, "0"))
      .select(finalColumns.map(col): _*)
  }
}
