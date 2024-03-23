package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.ProcessedTables
import com.javi.personal.wallascala.processor.tables.Properties._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class PisosProperties(date: LocalDate)(implicit spark: SparkSession) extends Properties(date) {

  override protected val datasetName: ProcessedTables = ProcessedTables.PISOS_PROPERTIES

  private object sources {
    val sanitedPisosProperties: DataFrame = readSanited("pisos", "properties", date)
  }

  override protected def build(): DataFrame = {

    sources.sanitedPisosProperties
      .withColumn(Surface, col("size"))
      .withColumn(Bathrooms, col("bathrooms"))
      .withColumn(Link, concat(lit("https://www.pisos.com"), col("url")))
      .withColumn(Source, lit("pisos"))
      .withColumn(CreationDate, lit(null)) // TODO: get creation date from fotocasa
      .withColumn(Currency, lit("EUR"))
      .withColumn(Elevator, lit(null))
      .withColumn(Garage, lit(null))
      .withColumn(Garden, lit(null))
      .withColumn(Country, lit("ES"))
      .withColumn(PostalCode, lit(null))
      .withColumn(Province, lit(null))
      .withColumn(Region, lit(null))
      .withColumn(ModificationDate, lit(null))
      .withColumn(Pool, lit(null))
      .withColumn(Terrace, lit(null))
      .withColumn(ExtractedDate, lit(null))
      .dropDuplicates(Title, Price, Description, Surface, Operation)

  }

}
