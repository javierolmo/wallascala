package com.javi.personal.wallascala.processor

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class ProcessorTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = null

  "build" should "find the ETL for table name 'apartment_investment_analysis'" in {
    val config = ProcessorConfig("apartment_investment_analysis", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "ApartmentInvestmentAnalysis"
  }

  it should "find the ETL for table name 'price_changes'" in {
    val config = ProcessorConfig("price_changes", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "PriceChanges"
  }

  it should "find the ETL for table name 'wallapop_properties'" in {
    val config = ProcessorConfig("wallapop_properties", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "WallapopProperties"
  }

  it should "find the ETL for table name 'wallapop_properties_snapshots'" in {
    val config = ProcessorConfig("wallapop_properties_snapshots", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "WallapopPropertiesSnapshots"
  }

  it should "find the ETL for table name 'properties'" in {
    val config = ProcessorConfig("properties", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "Properties"
  }

  it should "find the ETL for table name 'postal_code_analysis'" in {
    val config = ProcessorConfig("postal_code_analysis", LocalDate.now(), "ANY_VALUE")

    val processor = Processor.build(config)

    processor.getClass.getSimpleName shouldEqual "PostalCodeAnalysis"
  }
}
