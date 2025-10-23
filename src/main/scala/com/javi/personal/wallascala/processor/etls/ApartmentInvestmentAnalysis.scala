package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.APARTMENT_INVESTMENT_ANALYSIS)
case class ApartmentInvestmentAnalysis(config: ProcessorConfig, override val dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {
  override protected val schema: StructType = StructType(Seq()) // TODO: fill this

  private lazy val properties = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date))
  private lazy val postalCodeAnalysis = readProcessed(ProcessedTables.PROPERTIES, Some(config.date))

  override protected def build(): DataFrame = properties
}
