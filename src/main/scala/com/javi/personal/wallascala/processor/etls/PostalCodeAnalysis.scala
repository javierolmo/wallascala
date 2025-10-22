package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.PostalCodeAnalysis._
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.POSTAL_CODE_ANALYSIS)
case class PostalCodeAnalysis (config: ProcessorConfig)(implicit spark: SparkSession) extends Processor(config) {

  // override protected val finalColumns: Array[String] = Array(City, PostalCode, Type, Operation, AveragePrice, AverageSurface, AveragePriceM2, Count) // TODO

  private def getProperties: DataFrame =
    readProcessed(ProcessedTables.PROPERTIES)
      .filter(col(Properties.Date).leq(config.date))
      .withColumn("row_number", row_number().over(Window.partitionBy(Properties.Id).orderBy(col(Properties.Date).desc)))
      .filter(col("row_number") === 1)

  override protected def build(): DataFrame = {
    val properties = getProperties
    
    properties
      .withColumn(Properties.Surface, when(col(Properties.Surface) === 0, null).otherwise(col(Properties.Surface)))
      .withColumn(Properties.Price, when(col(Properties.Price) === 0, null).otherwise(col(Properties.Price)))
      .withColumn("price_m2", col(Properties.Price) / col(Properties.Surface))
      .groupBy(Properties.PostalCode, Properties.Type, Properties.Operation)
      .agg(
        first(Properties.City).as(City),
        round(avg(Properties.Price), 2).as(AveragePrice),
        round(avg(Properties.Surface), 2).as(AverageSurface),
        round(avg("price_m2"), 2).as(AveragePriceM2),
        count(Properties.Id).as(Count)
      )
  }
}

object PostalCodeAnalysis {
  val City = "city"
  val PostalCode = "postal_code"
  val Type = "type"
  val Operation = "operation"
  val AveragePrice = "average_price"
  val AverageSurface = "average_surface"
  val AveragePriceM2 = "average_price_m2"
  val Count = "count"
}
