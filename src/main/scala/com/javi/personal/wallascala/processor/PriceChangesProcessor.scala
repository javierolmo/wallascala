package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.PathBuilder
import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, collect_list, current_date, element_at, lit, lpad, row_number, size, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class PriceChangesProcessor(spark: SparkSession) extends Processor(spark) {

  private val log = LogManager.getLogger(getClass)
  override protected val datasetName: String = "price_changes"
  override protected val finalColumns: Array[String] = Array(
    "id", "title", "price_changes", "price_history", "first_price", "last_price", "discount", "link", "year", "month",
    "day"
  )

  override protected def build(): DataFrame = {
    val currentDate = LocalDate.now()
    val propertiesDF = spark.read.parquet(PathBuilder.buildProcessedPath("properties").url)
    val activePropertyIds = propertiesDF.filter(col("extracted_date") === current_date()).select("id").distinct().collect().map(_.getString(0))
    log.info(s"Active properties: ${activePropertyIds.length}")
    propertiesDF
      .filter(col("city") === "Vigo")
      .withColumn("row_number", row_number().over(Window.partitionBy("id", "price").orderBy(col("extracted_date").asc)))
      .filter(col("row_number") === lit(1))
      .withColumn("price_history", collect_list(struct(col("extracted_date"), col("price"))).over(Window.partitionBy("id").orderBy(col("extracted_date").asc)))
      .withColumn("price_changes", size(col("price_history")) - 1)
      .filter(col("price_changes") > 0)
      .withColumn("first_price", col("price_history")(0)("price"))
      .withColumn("last_price", element_at(col("price_history"), -1)("price"))
      .withColumn("discount", bround((col("first_price") - col("last_price")) / col("first_price"), 2))
      .withColumn("year", lpad(lit(currentDate.getYear), 4, "0"))
      .withColumn("month", lpad(lit(currentDate.getMonthValue), 2, "0"))
      .withColumn("day", lpad(lit(currentDate.getDayOfMonth), 2, "0"))
      .select(finalColumns.map(col): _*)
      .filter(col("id").isin(activePropertyIds: _*))
      .coalesce(1)
  }
}
