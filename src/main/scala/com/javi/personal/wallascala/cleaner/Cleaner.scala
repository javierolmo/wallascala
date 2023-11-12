package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.PathBuilder
import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, CleanerMetadataField}
import com.javi.personal.wallascala.cleaner.validator.ValidationResult
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

class Cleaner(spark: SparkSession) {

  def execute(source: String, datasetName: String, partitionBy: Seq[String] = Seq("year", "month", "day")): Unit = {
    val location = PathBuilder.buildRawPath(source, datasetName)
    val inputDF: DataFrame = spark.read.parquet(location.url)

    val cleanerMetadata = CleanerMetadata.findByCatalogItem(source, datasetName).get
    val validated: ValidationResult = validate(inputDF, cleanerMetadata)


    validated.validRecords.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy(partitionBy: _*)
      .option("path", PathBuilder.buildSanitedPath(source, datasetName).url)
      .saveAsTable(s"sanited.${source}_$datasetName")
    validated.invalidRecords.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy(partitionBy: _*)
      .option("path", PathBuilder.buildExcludedPath(source, datasetName).url)
      .saveAsTable(s"sanited_excluded.${source}_$datasetName")
  }

  def execute(source: String, datasetName: String, localDate: LocalDate): Unit = {
    val location = PathBuilder.buildRawPath(source, datasetName).cd(localDate)
    val inputDF: DataFrame = spark.read.parquet(location.url)

    val cleanerMetadata = CleanerMetadata.findByCatalogItem(source, datasetName).get
    val validated: ValidationResult = validate(inputDF, cleanerMetadata).withYearMonthDay(localDate)

    validated.validRecords.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy(Seq("year", "month", "day"): _*)
      .option("path", PathBuilder.buildSanitedPath(source, datasetName).url)
      .saveAsTable(s"sanited.${source}_$datasetName")
    validated.invalidRecords.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy(Seq("year", "month", "day"): _*)
      .option("path", PathBuilder.buildExcludedPath(source, datasetName).url)
      .saveAsTable(s"sanited_excluded.${source}_$datasetName")
  }

  def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

    def cleanField(df: DataFrame, field: CleanerMetadataField): DataFrame = {
      df.withColumn(field.name, field.genericFieldCleaner(col(field.name)))
    }

    val dfCleaned = metadata.fields
      .foldLeft(inputDF)(cleanField)
      .select(metadata.fields.map(field => col(field.name)): _*)

    val dfInvalidRecords = dfCleaned
      .filter(metadata.fields.map(field => col(s"${field.name}.error").isNotNull).reduce(_ || _))
      .select(dfCleaned.columns.map(x => col(x+".error").as(x)): _*)
    val dfValidRecords = dfCleaned
      .filter(metadata.fields.map(field => col(s"${field.name}.error").isNull).reduce(_ && _))
      .select(dfCleaned.columns.map(x => col(x+".result").as(x)): _*)


    ValidationResult(dfValidRecords, dfInvalidRecords)
  }


}
