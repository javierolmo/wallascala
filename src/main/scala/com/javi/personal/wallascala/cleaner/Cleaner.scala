package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, CleanerMetadataField}
import com.javi.personal.wallascala.cleaner.validator.ValidationResult
import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object Cleaner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.build()
    val cleaner = new Cleaner(spark)
    cleaner.execute("wallapop", "properties", LocalDate.of(2023, 6, 18))
  }
}

class Cleaner(spark: SparkSession) {

  def execute(source: String, datasetName: String, localDate: LocalDate): Unit = {
    val location = PathBuilder.buildRawPath(source, datasetName).cd(localDate)
    val inputDF: DataFrame = spark.read.parquet(location.url)

    val cleanerMetadata = CleanerMetadata.findByCatalogItem(source, datasetName).get
    val validated: ValidationResult = validate(inputDF, cleanerMetadata).withYearMonthDay(localDate)

    validated.validRecords.write
      .mode(SaveMode.Overwrite)
      .partitionBy(Seq("year", "month", "day"): _*)
      .parquet(PathBuilder.buildSanitedPath(source, datasetName).url)
    validated.invalidRecords.write
      .mode(SaveMode.Overwrite)
      .partitionBy(Seq("year", "month", "day"): _*)
      .parquet(PathBuilder.buildExcludedPath(source, datasetName).url)
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
