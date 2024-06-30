package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

@ETL(table = ProcessedTables.APARTMENT_INVESTMENT_ANALYSIS)
case class ApartmentInvestmentAnalysis(dateOption: Option[LocalDate])(implicit spark: SparkSession) extends Processor(dateOption.get) {
  override protected val schema: StructType = StructType(Seq()) // TODO: fill this

  private val properties = readProcessed(ProcessedTables.WALLAPOP_PROPERTIES, dateOption)
  private val postalCodeAnalysis = readProcessed(ProcessedTables.PROPERTIES, dateOption)

  override protected def build(): DataFrame = {
    properties
  }
}
