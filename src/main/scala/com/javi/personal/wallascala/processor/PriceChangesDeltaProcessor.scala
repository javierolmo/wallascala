package com.javi.personal.wallascala.processor

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class PriceChangesDeltaProcessor(spark: SparkSession) extends Processor(spark) {

  private val properties = readProcessed("properties")

  override protected val datasetName: String = "price_changes_delta"
  override protected val finalColumns: Array[String] = Array(
    "id", "title", "previous_price", "new_price", "link", "year", "month", "day"
  )

  override protected def build(date: LocalDate): DataFrame = {
    val lastTwoDaysProperties = properties.filter(ymdCondition(date) || ymdCondition(date.plusDays(-1)))
    val windowSpec = Window.partitionBy("id").orderBy(col("extracted_date").asc)
    lastTwoDaysProperties
      .withColumn("previous_price", lag("price", 1).over(windowSpec))
      .filter(ymdCondition(date))
      .withColumn("new_price", col("price"))
      .filter(col("previous_price") =!= col("new_price"))
  }
}
