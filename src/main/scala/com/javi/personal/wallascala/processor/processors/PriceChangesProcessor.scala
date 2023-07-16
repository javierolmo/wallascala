package com.javi.personal.wallascala.processor.processors

import com.javi.personal.wallascala.processor.Processor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lpad}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PriceChangesProcessor(dateOption: Option[LocalDate])(implicit spark: SparkSession) extends Processor(spark) {

  private val properties =
    if (dateOption.isDefined) {
      val date = dateOption.get
      readProcessed("properties").filter(ymdCondition(date) || ymdCondition(date))
    } else
      readProcessed("properties")

  override protected val coalesce: Option[Int] = Some(1)
  override protected val datasetName: String = "price_changes"
  override protected val finalColumns: Array[String] = Array(
    "id", "title", "previous_price", "new_price", "link", "year", "month", "day"
  )

  override protected def build(): DataFrame = {
    val windowSpec = Window.partitionBy("id").orderBy(col("extracted_date").asc)
    properties
      .withColumn("previous_price", lag("price", 1).over(windowSpec))
      .withColumn("new_price", col("price"))
      .filter(col("previous_price").isNotNull)
      .filter(col("previous_price") =!= col("new_price"))
      .withColumn("year", lpad(col("year"), 4, "0"))
      .withColumn("month", lpad(col("month"), 2, "0"))
      .withColumn("day", lpad(col("day"), 2, "0"))
  }
}
