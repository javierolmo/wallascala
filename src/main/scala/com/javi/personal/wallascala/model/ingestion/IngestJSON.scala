package com.javi.personal.wallascala.model.ingestion

import com.javi.personal.wallascala.model.SparkSessionWrapper
import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.services.impl.blob.model.{ReadConfig, StorageAccountLocation, WriteConfig}
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, dayofmonth, explode, lpad, month, regexp_replace, to_date, year}

object IngestJSON extends SparkSessionWrapper {

  val catalogItem = DataCatalog.PISO_WALLAPOP
  val blobService = BlobService(SecretService())

  def main(args: Array[String]): Unit = {

    val stagingJSON = blobService.read(catalogItem.stagingLocation, ReadConfig(format = "json", options = Map("multiline" -> "true")))
      .withColumn("element", explode(col("elements")))
      .select("element.*","city","source","date")
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("year", lpad(year(col("date")), 4, "0"))
      .withColumn("month", lpad(month(col("date")), 2, "0"))
      .withColumn("day", lpad(dayofmonth(col("date")), 2, "0"))

    val writeConfig = WriteConfig(
      format = "parquet",
      saveMode = SaveMode.Overwrite,
      options = Map(),
      partitionColumns = Seq("year", "month", "day")
    )

    blobService.write(stagingJSON, catalogItem.rawLocation, writeConfig)
  }

}
