package com.javi.personal.wallascala.model

import com.javi.personal.wallascala.model.services.impl.blob.model.{ReadConfig, StorageAccountLocation}
import com.javi.personal.wallascala.model.services.impl.database.DataSource
import com.javi.personal.wallascala.model.services.{BlobService, DatabaseService, SecretService}
import org.apache.spark.sql.functions.{bround, col, current_date, date_format, element_at, to_json}

object Egestion {

  def main(args: Array[String]): Unit = {

    val blobService = BlobService(SecretService())
    val databaseService = DatabaseService(DataSource(
      host = "tfgsqlserver.database.windows.net",
      port = "1433",
      database = "tfg-sqldb",
      user = "jolmo60",
      password = "47921093tT?"
    ))

    writePriceChanges(blobService, databaseService)
  }

  def writePriceChanges(blobService: BlobService, databaseService: DatabaseService): Unit = {
    val locationPriceChanges = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/price_changes",
      v2 = true
    )

    val price_changes = blobService.read(locationPriceChanges, ReadConfig(format = "parquet"))
      .withColumn("price_history", to_json(col("price_history")))

    price_changes.write
      .format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:sqlserver://tfgsqlserver.database.windows.net;databaseName=tfg-sqldb;")
      .option("dbtable", "price_changes")
      .option("user", "jolmo60")
      .option("password", "47921093tT?")
      .save()
  }

  def writeProperties(blobService: BlobService) = {
    val locationProperties = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/properties",
      v2 = true
    )

    val readConfig = ReadConfig(format = "parquet")
    val properties = blobService.read(locationProperties, readConfig)

    properties
      .filter(col("extracted_date") === date_format(current_date(), "yyyy-MM-dd"))
      .write
      .format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:sqlserver://tfgsqlserver.database.windows.net;databaseName=tfg-sqldb;")
      .option("dbtable", "properties")
      .option("user", "jolmo60")
      .option("password", "47921093tT?")
      .save()
  }

  def writePropertiesHistory(blobService: BlobService) = {
    val locationProperties = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/properties",
      v2 = true
    )

    val readConfig = ReadConfig(format = "parquet")
    val properties = blobService.read(locationProperties, readConfig)

    properties.write
      .format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:sqlserver://tfgsqlserver.database.windows.net;databaseName=tfg-sqldb;")
      .option("dbtable", "properties_history")
      .option("user", "jolmo60")
      .option("password", "47921093tT?")
      .save()
  }

}
