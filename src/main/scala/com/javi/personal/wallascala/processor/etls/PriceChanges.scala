package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.etls.PriceChanges._
import com.javi.personal.wallascala.processor.etls.Properties.Id
import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions.{col, not, round}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PRICE_CHANGES)
case class PriceChanges(config: ProcessorConfig, override val dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

  override protected val schema: StructType = StructType(Array(
    StructField(Id, StringType),
    StructField(PreviousPrice, IntegerType),
    StructField(NewPrice, IntegerType),
    StructField(Discount, DoubleType)
  ))

  private object sources {
    lazy val todayProperties: DataFrame = dataSourceProvider.readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date))
      .select(Properties.Id, Properties.Price).as("tp")
    lazy val yesterdayProperties: DataFrame = dataSourceProvider.readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date.minusDays(1)))
      .select(Properties.Id, Properties.Price).as("yp")
  }

  override protected def build(): DataFrame =
    sources.todayProperties
      .join(sources.yesterdayProperties, Id)
      .filter(sources.yesterdayProperties(Properties.Price).notEqual(sources.todayProperties(Properties.Price)))
      .withColumn(PreviousPrice, sources.yesterdayProperties(Properties.Price))
      .withColumn(NewPrice, sources.todayProperties(Properties.Price))
      .withColumn(DiscountRate, round((col(NewPrice) - col(PreviousPrice)) / col(PreviousPrice), 4))
      .withColumn(Discount, col(NewPrice) - col(PreviousPrice))

}

object PriceChanges {
  val Id = "id"
  val PreviousPrice = "previous_price"
  val NewPrice = "new_price"
  val Discount = "discount"
  val DiscountRate = "discount_rate"
}
