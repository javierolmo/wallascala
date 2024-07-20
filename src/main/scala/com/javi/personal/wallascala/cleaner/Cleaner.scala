package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, MetadataCatalog, ValidationResult}
import com.javi.personal.wallascala.utils.reader.SparkFileReader
import com.javi.personal.wallascala.utils.writers.SparkFileWriter
import org.apache.spark.sql.functions.{array, array_except, col, flatten, lit, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleaner {

  def execute(config: CleanerConfig, metadataCatalog: MetadataCatalog = MetadataCatalog.default())(implicit spark: SparkSession): Unit = {
    val cleanerMetadata = metadataCatalog.findByCatalogItem(config.id).getOrElse(throw new IllegalArgumentException(s"No metadata found for id '${config.id}'"))
    val rawDF: DataFrame = SparkFileReader.read(config.sourcePath)

    val result = validate(rawDF, cleanerMetadata)

    SparkFileWriter.write(result.validRecords, config.targetPath)
    SparkFileWriter.write(result.invalidRecords, config.targetPathExclusions)
  }

  private def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

    def cleanField(df: DataFrame, field: FieldCleaner): DataFrame = {
      val (result, error) = field.clean(col(field.name))
      df
        .withColumn(f"${field.name}_result", result)
        .withColumn(f"${field.name}_error", error)
    }

    val dfCleaned = metadata.fields
      .foldLeft(inputDF)(cleanField)
      .withColumn("errors", {
        val errorsArray = flatten(array(metadata.fields.map(field => col(s"${field.name}_error")):_*))
        val exclusions = array(lit(null))
        array_except(errorsArray, exclusions)
      })
      .withColumn("hasErrors", size(col("errors")) > 0)

    val dfInvalidRecords = dfCleaned
      .filter(col("hasErrors"))
      .select(Seq(col("errors")) ++ metadata.fields.map(x => col(x.name)): _*)
    val dfValidRecords = dfCleaned
      .filter(!col("hasErrors"))
      .select(metadata.fields.map(x => col(x.name+"_result").as(x.name)): _*)


    ValidationResult(dfValidRecords, dfInvalidRecords)
  }


}
