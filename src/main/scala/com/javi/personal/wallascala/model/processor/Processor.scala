package com.javi.personal.wallascala.model.processor

import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.services.impl.blob.model.StorageAccountLocation
import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

class Processor(spark: SparkSession) {

  private val log = LogManager.getLogger(getClass)

  def process(goldName: String): Unit = {
    goldName match {
      case "properties" => processProperties()
      case "price_changes" => processPriceChanges(goldName)
    }
  }

  def processProperties(): Unit = {
    val FINAL_COLUMNS = Array(
      "id", "title", "price", "surface", "rooms", "bathrooms", "link", "source", "creation_date", "currency",
      "elevator", "garage", "garden", "city", "country", "postal_code", "modification_date", "operation", "pool",
      "description", "terrace", "type", "extracted_date", "year", "month", "day"
    )


    val wallapopDF = spark.read.parquet(DataCatalog.PISO_WALLAPOP.sanitedLocation.url)
      .withColumn("city", col("location__city"))
      .withColumn("country", col("location__country_code"))
      .withColumn("postal_code", col("location__postal_code"))
      .withColumn("description", col("storytelling"))
      .withColumn("link", concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn("extracted_date", col("date"))
      .withColumn("year", lpad(col("year"), 4, "0"))
      .withColumn("month", lpad(col("month"), 2, "0"))
      .withColumn("day", lpad(col("day"), 2, "0"))
      .select(FINAL_COLUMNS.map(col): _*)

    val resultDF = wallapopDF

    resultDF.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(processedLocation("properties").url)
  }

  def processPriceChanges(tableName: String): Unit = {
    val currentDate = LocalDate.now()
    val finalColumns = Array(
      "id", "title", "price_changes", "price_history", "first_price", "last_price", "discount", "link", "year", "month", "day"
    )
    val propertiesDF = spark.read.parquet(processedLocation("properties").url)
    val activePropertyIds = propertiesDF.filter(col("extracted_date") === current_date()).select("id").distinct().collect().map(_.getString(0))
    log.info(s"Active properties: ${activePropertyIds.length}")
    val df = propertiesDF
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

    df.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(processedLocation(tableName).url)
  }


  private def processedLocation(datasetName: String): StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = s"processed/$datasetName",
    v2 = true
  )

}
