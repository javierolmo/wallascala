package com.javi.personal.wallascala.model.egestor

import com.javi.personal.wallascala.model.services.impl.blob.model.StorageAccountLocation
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, date_format, to_json}

import java.time.LocalDate

class Egestor(spark: SparkSession) {

  private val GOLD_PROPERTIES_LOCATION: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = "processed/properties",
    v2 = true
  )

  private val GOLD_PRICECHANGES_LOCATION: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = "processed/price_changes",
    v2 = true
  )

  def writePriceChanges(date: LocalDate = LocalDate.now()): Unit = {
    val price_changes = spark.read.parquet(GOLD_PRICECHANGES_LOCATION.url)
      .withColumn("price_history", to_json(col("price_history")))
      .filter(col("year") === date.getYear)
      .filter(col("month") === date.getMonthValue)
      .filter(col("day") === date.getDayOfMonth)
      .drop("year", "month", "day")

    price_changes.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", "jdbc:sqlserver://tfgsqlserver.database.windows.net;databaseName=tfg-sqldb;")
      .option("dbtable", "price_changes")
      .option("user", "jolmo60")
      .option("password", "47921093tT?")
      .save()
  }

  def writeProperties(): Unit = {

    val properties = spark.read.parquet(GOLD_PROPERTIES_LOCATION.url)

    properties
      .write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", "jdbc:sqlserver://tfgsqlserver.database.windows.net;databaseName=tfg-sqldb;")
      .option("dbtable", "properties")
      .option("user", "jolmo60")
      .option("password", "47921093tT?")
      .save()
  }

}
