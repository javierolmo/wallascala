package com.javi.personal
package wallascala.extractor

import com.javi.personal.wallascala.catalog.CatalogItem
import com.javi.personal.wallascala.services.BlobService
import com.javi.personal.wallascala.services.impl.blob.model.{StorageAccountLocation, WriteConfig}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class Extractor(blobService: BlobService) {

  def extract(): Unit

  def writeToDatalake(dataFrame: DataFrame, catalogItem: CatalogItem): Unit = {
    val date = LocalDate.now()
    val yearString = DateTimeFormatter.ofPattern("yyyy").format(date)
    val monthString = DateTimeFormatter.ofPattern("MM").format(date)
    val dayString = DateTimeFormatter.ofPattern("dd").format(date)
    val dfWithDate = dataFrame
      .withColumn("year", lit(yearString))
      .withColumn("month", lit(monthString))
      .withColumn("day", lit(dayString))
    val location = catalogItem.rawLocation
    val writeConfig = WriteConfig(
      format = "parquet",
      saveMode = SaveMode.Overwrite,
      options = Map(),
      partitionColumns = Seq("year", "month", "day")
    )
    blobService.write(dfWithDate, location, writeConfig)
  }

}
