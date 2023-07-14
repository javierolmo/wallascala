package com.javi.personal.wallascala.ingestion

import com.javi.personal.wallascala.{SparkSessionFactory, StorageAccountLocation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Ingestor {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.build()
    val ingestor = new Ingestor(spark)
    ingestor.ingest("fotocasa", "apartment_sales")
  }

}

class Ingestor(spark: SparkSession) {

  def ingest(source: String, datasetName: String): Unit = {

    val stagingJSON = spark.read
      .format("json")
      .option("multiline", "true")
      .load(stagingLocation(source, datasetName).url)

    val transformedDF = stagingJSON
      .withColumn("element", explode(col("elements")))
      .select("element.*", "city", "source", "date")
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("year", lpad(year(col("date")), 4, "0"))
      .withColumn("month", lpad(month(col("date")), 2, "0"))
      .withColumn("day", lpad(dayofmonth(col("date")), 2, "0"))

    transformedDF.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(rawLocation(source, datasetName).url)

  }

  private def stagingLocation(source: String, datasetName: String): StorageAccountLocation =
    StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = s"staging/$source/$datasetName",
      v2 = true)

  private def rawLocation(source: String, datasetName: String): StorageAccountLocation =
    StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = s"raw/$source/$datasetName",
      v2 = true)

}
