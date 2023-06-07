package com.javi.personal.wallascala.model.ingestion

import com.javi.personal.wallascala.model.SparkSessionWrapper
import com.javi.personal.wallascala.model.catalog.{CatalogItem, DataCatalog}
import com.javi.personal.wallascala.model.services.impl.blob.model.{ReadConfig, StorageAccountLocation, WriteConfig}
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, dayofmonth, explode, lpad, month, regexp_replace, to_date, year}

class Ingestor(spark: SparkSession) {

  def ingest(catalogItem: CatalogItem): Unit = {

    val stagingJSON = spark.read.format("json").option("multiline", "true").load(catalogItem.stagingLocation.url)

    val transformedDF = stagingJSON
      .withColumn("element", explode(col("elements")))
      .select("element.*","city","source","date")
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("year", lpad(year(col("date")), 4, "0"))
      .withColumn("month", lpad(month(col("date")), 2, "0"))
      .withColumn("day", lpad(dayofmonth(col("date")), 2, "0"))

    transformedDF.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(catalogItem.rawLocation.url)

  }

}
