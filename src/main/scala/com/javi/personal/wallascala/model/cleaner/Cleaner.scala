package com.javi.personal.wallascala.model.cleaner

import com.javi.personal.wallascala.model.catalog.CatalogItem
import com.javi.personal.wallascala.model.cleaner.model.{CleanerMetadata, CleanerMetadataField}
import com.javi.personal.wallascala.model.cleaner.validator.ValidationResult
import com.javi.personal.wallascala.model.services.BlobService
import com.javi.personal.wallascala.model.services.impl.blob.model.{ReadConfig, WriteConfig}
import org.apache.spark.sql.functions.col
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
    val validated: ValidationResult = validate(inputDF, cleanerMetadata).withYearMonthDay(localDate)

    blobService.write(validated.validRecords, catalogItem.sanitedLocation, writeConfig)
    blobService.write(validated.invalidRecords, catalogItem.excludedLocation, writeConfig)
  }

  def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

    def cleanField(df: DataFrame, field: CleanerMetadataField): DataFrame = {
      df.withColumn(field.name, field.buildUDF.apply(col(field.name)))
    }

    val dfCleaned = metadata.fields
      .foldLeft(inputDF)(cleanField)
      .select(metadata.fields.map(field => col(field.name)): _*)

    val dfInvalidRecords = dfCleaned
      .filter(metadata.fields.map(field => col(s"${field.name}._2").isNotNull).reduce(_ || _))
      .select(dfCleaned.columns.map(x => col(x+"._2").as(x)): _*)
    val dfValidRecords = dfCleaned
      .filter(metadata.fields.map(field => col(s"${field.name}._2").isNull).reduce(_ && _))
      .select(dfCleaned.columns.map(x => col(x+"._1").as(x)): _*)


    ValidationResult(dfValidRecords, dfInvalidRecords)
  }


}
