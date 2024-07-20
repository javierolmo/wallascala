package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.PriceChanges._
import com.javi.personal.wallascala.processor.etls.Properties.Id
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PRICE_CHANGES)
case class PriceChanges(config: ProcessorConfig)(implicit spark: SparkSession) extends Processor(config) {

  override protected val writerCoalesce: Option[Int] = Some(1)
  override protected val schema: StructType = StructType(Array(
    StructField(Id, StringType),
    StructField(PreviousPrice, IntegerType),
    StructField(NewPrice, IntegerType),
    StructField(Discount, DoubleType)
  ))

  private object sources {
    lazy val todayProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date))
      .select(Properties.Id, Properties.Price).as("tp")
    lazy val yesterdayProperties: DataFrame = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date.minusDays(1)))
      .select(Properties.Id, Properties.Price).as("yp")
  }

  override protected def build(): DataFrame = {
    val result = sources.todayProperties
      .join(sources.yesterdayProperties, Id)
      .filter(sources.yesterdayProperties(Properties.Price) =!= sources.todayProperties(Properties.Price))
      .withColumn(PreviousPrice, sources.yesterdayProperties(Properties.Price))
      .withColumn(NewPrice, sources.todayProperties(Properties.Price))
      .withColumn(DiscountRate, round((col(NewPrice) - col(PreviousPrice)) / col(PreviousPrice), 4))
      .withColumn(Discount, col(NewPrice) - col(PreviousPrice))
    result
  }

}

object PriceChanges {
  val Id = "id"
  val PreviousPrice = "previous_price"
  val NewPrice = "new_price"
  val Discount = "discount"
  val DiscountRate = "discount_rate"
}
