package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.ProcessedTables
import com.javi.personal.wallascala.processor.tables.Properties._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

class WallapopProperties(date: LocalDate)(implicit spark: SparkSession) extends Properties(date) {

  override protected val datasetName: ProcessedTables = ProcessedTables.WALLAPOP_PROPERTIES

  object sources {
    val sanitedWallapopProperties: DataFrame = readSanited("wallapop", "properties", date)
    val sanitedProvinces: DataFrame = readSanited("opendatasoft", "provincias-espanolas")
  }

  override protected def build(): DataFrame = {
    sources.sanitedWallapopProperties
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType)/1000).cast(IntegerType))
      .join(sources.sanitedProvinces.as("p"), col("province_code") === sources.sanitedProvinces("codigo").cast(IntegerType), "left")
      .withColumnRenamed("location__city", City)
      .withColumnRenamed("location__country_code", Country)
      .withColumnRenamed("location__postal_code", PostalCode)
      .withColumnRenamed("provincia", Province)
      .withColumnRenamed("ccaa", Region)
      .withColumnRenamed("storytelling", Description)
      .withColumn(Source, lit("wallapop"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, to_date(col(CreationDate)))
      .withColumn(ModificationDate, to_date(col(ModificationDate)))
      .withColumn(ExtractedDate, to_date(col("date")))
      .dropDuplicates(Title, Price, Description, Surface, Operation)
  }

}
