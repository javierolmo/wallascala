package com.javi.personal.wallascala.processor.tables

import com.javi.personal.wallascala.processor.{ProcessedTables, Processor}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

case class ApartmentInvestmentAnalysis(dateOption: Option[LocalDate])(implicit spark: SparkSession) extends Processor(dateOption.get) {
  override protected val datasetName: ProcessedTables = ProcessedTables.APARTMENT_INVESTMENT_ANALYSIS
  override protected val finalColumns: Array[String] = ???

  private val properties = dateOption match {
    case Some(date) => readProcessed("properties").filter(ymdCondition(date))
    case None => readProcessed("properties")
  }

  private val postalCodeAnalysis = dateOption match {
    case Some(date) => readProcessed("properties").filter(ymdCondition(date))
    case None => readProcessed("properties")
  }

  override protected def build(): DataFrame = {
    properties
  }
}
