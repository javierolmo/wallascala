package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.APARTMENT_INVESTMENT_ANALYSIS)
case class ApartmentInvestmentAnalysis(config: ProcessorConfig)(implicit spark: SparkSession) extends Processor(config) {
  override protected val schema: StructType = StructType(Seq()) // TODO: fill this

  private def getProperties: DataFrame = 
    readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, Some(config.date))
  
  private def getPostalCodeAnalysis: DataFrame = 
    readProcessed(ProcessedTables.PROPERTIES, Some(config.date))

  override protected def build(): DataFrame = getProperties
}
