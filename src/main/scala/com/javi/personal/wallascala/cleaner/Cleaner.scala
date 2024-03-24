package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, CleanerMetadataField, ValidationResult}
import com.javi.personal.wallascala.utils.reader.SparkFileReader
import com.javi.personal.wallascala.utils.writers.SparkFileWriter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleaner {

  def execute(config: CleanerConfig)(implicit spark: SparkSession): Unit = {
    val cleanerMetadata = CleanerMetadata.findByCatalogItem(config.id).getOrElse(throw new IllegalArgumentException(s"No metadata found for id '${config.id}'"))
    val rawDF: DataFrame = SparkFileReader.read(config.sourcePath)

    val result = validate(rawDF, cleanerMetadata)

    SparkFileWriter.write(result.validRecords, config.targetPath)
    SparkFileWriter.write(result.invalidRecords, config.targetPathExclusions)
  }

  private def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

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
