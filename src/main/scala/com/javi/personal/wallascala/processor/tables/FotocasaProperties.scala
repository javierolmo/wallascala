package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.ProcessedTables
import com.javi.personal.wallascala.processor.tables.Properties._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class FotocasaProperties(date: LocalDate)(implicit spark: SparkSession) extends Properties(date) {

  override protected val datasetName: ProcessedTables = ProcessedTables.FOTOCASA_PROPERTIES

  private object sources {
    val sanitedFotocasaProperties: DataFrame = readSanited("fotocasa", "properties").filter(ymdCondition(date))
  }

  override protected def build(): DataFrame = {

    sources.sanitedFotocasaProperties
      .withColumn(Surface, col("features__size"))
      .withColumn(Rooms, col("features__rooms"))
      .withColumn(Bathrooms, col("features__bathrooms"))
      .withColumn(Link, concat(lit("https://www.fotocasa.es"), col("url")))
      .withColumn(Source, lit("fotocasa"))
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
      .withColumn(Description, lit(null))
      .withColumn(Terrace, lit(null))
      .withColumn(ExtractedDate, lit(null))
      .dropDuplicates(Title, Price, Description, Surface, Operation)

  }

}
