package com.javi.personal
package wallascala.cleaner

import com.javi.personal.wallascala.catalog.CatalogItem
import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, CleanerMetadataField}
import com.javi.personal.wallascala.cleaner.validator.ValidationResult
import com.javi.personal.wallascala.services.BlobService
import com.javi.personal.wallascala.services.impl.blob.model.{ReadConfig, WriteConfig}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate

class Cleaner(blobService: BlobService) {

  private val readConfig: ReadConfig = ReadConfig(
    format = "parquet",
  )

  private val writeConfig: WriteConfig = WriteConfig(
    format = "parquet",
    saveMode = SaveMode.Overwrite,
    partitionColumns = Seq("year", "month", "day")
  )

  def execute(catalogItem: CatalogItem, localDate: LocalDate) = {
    val location = catalogItem.rawLocation.cd(localDate)
    val inputDF: DataFrame = blobService.read(location, readConfig)

    val cleanerMetadata = CleanerMetadata.findByCatalogItem(catalogItem).get
    val result: ValidationResult = validate(inputDF, cleanerMetadata).withYearMonthDay(localDate)

    blobService.write(result.validRecords, catalogItem.sanitedLocation, writeConfig)
    blobService.write(result.invalidRecords, catalogItem.excludedLocation, writeConfig)
  }

  def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

    /*val dataFrame = inputDF
      .withColumn("error_message", lit(null).cast(StringType))
      .withColumn("error_column", lit(null).cast(StringType))
      .withColumn("error_value", lit(null).cast(StringType))


    def transformField(field: CleanerMetadataField, df: DataFrame): DataFrame = {
      null
    }

    val result = metadata.fields.foldRight(inputDF)(
      (field: CleanerMetadataField, result: DataFrame) => transformField(field, result)
    )

    val selectedColumns = metadata.fields.map(_.name).map(fieldName => col(fieldName))
    val validRecords = result.select(selectedColumns: _*)

    val invalidRecords = result.select(col("error_message"), col("error_column"), col("error_value"))

    ValidationResult(validRecords, invalidRecords)
     */
    ValidationResult(inputDF, BlobService.spark.emptyDataFrame.withColumn("error_message", lit("").cast(StringType)))
  }


}
