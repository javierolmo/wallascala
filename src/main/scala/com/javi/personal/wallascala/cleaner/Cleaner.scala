package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, MetadataCatalog, ValidationResult}
import com.javi.personal.wallascala.utils.reader.SparkFileReader
import com.javi.personal.wallascala.utils.writers.SparkFileWriter
import org.apache.spark.sql.functions.{array, array_except, col, flatten, lit, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleaner extends com.javi.personal.wallascala.Logging {

  def execute(config: CleanerConfig, metadataCatalog: MetadataCatalog = MetadataCatalog.default())(implicit spark: SparkSession): Unit = {
    logger.info("Starting cleaner execution for id: {}", config.id)
    val cleanerMetadata = metadataCatalog.findByCatalogItem(config.id).getOrElse {
      logger.error("No metadata found for id '{}'", config.id)
      throw com.javi.personal.wallascala.WallaScalaException(s"No metadata found for id '${config.id}'")
    }
    logger.debug("Found metadata for id: {}", config.id)
    
    logger.info("Reading raw data from: {}", config.sourcePath)
    val rawDF: DataFrame = SparkFileReader.read(config.sourcePath)
    logger.info("Raw data read successfully, row count: {}", rawDF.count())

    logger.info("Validating data")
    val result = validate(rawDF, cleanerMetadata)
    logger.info("Validation complete. Valid records: {}, Invalid records: {}", 
      result.validRecords.count(), result.invalidRecords.count())

    logger.info("Writing valid records to: {}", config.targetPath)
    SparkFileWriter.write(result.validRecords, config.targetPath)
    logger.info("Writing invalid records to: {}", config.targetPathExclusions)
    SparkFileWriter.write(result.invalidRecords, config.targetPathExclusions)
    logger.info("Cleaner execution completed successfully")
  }

  private def validate(inputDF: DataFrame, metadata: CleanerMetadata): ValidationResult = {

    def cleanField(df: DataFrame, field: FieldCleaner): DataFrame = {
      val (error, result) = field.clean(col(field.name))
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
      .select((Seq(col("errors")) ++ metadata.fields.map(x => col(x.name))): _*)
    val dfValidRecords = dfCleaned
      .filter(!col("hasErrors"))
      .select(metadata.fields.map(x => col(s"${x.name}_result").as(x.name)): _*)


    ValidationResult(dfValidRecords, dfInvalidRecords)
  }


}
