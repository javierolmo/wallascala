package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessedTables, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class ApartmentInvestmentAnalysisIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ApartmentInvestmentAnalysisIntegrationTest")
    .getOrCreate()

  import spark.implicits._

  "ApartmentInvestmentAnalysis ETL" should "return wallapop properties data" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("apartment_investment_analysis", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Sample wallapop properties data (using StringType for dates)
    val wallapopPropertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("city", StringType),
      StructField("date", StringType)
    ))
    
    val wallapopData = Seq(
      Row("prop1", "Piso Madrid", 250000, 80, "Madrid", "2024-01-15"),
      Row("prop2", "Apartamento Barcelona", 300000, 100, "Barcelona", "2024-01-15")
    )
    
    val wallapopDF = spark.createDataFrame(
      spark.sparkContext.parallelize(wallapopData),
      wallapopPropertiesSchema
    )
    
    // Given: Sample properties data
    val propertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("city", StringType),
      StructField("date", StringType)
    ))
    
    val propertiesData = Seq(
      Row("prop3", "Casa Valencia", 180000, 120, "Valencia", "2024-01-15")
    )
    
    val propertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(propertiesData),
      propertiesSchema
    )
    
    // Register mock data sources
    mockDataSource.registerProcessedDataSource(ProcessedTables.WALLAPOP_PROPERTIES, Some(testDate), wallapopDF)
    mockDataSource.registerProcessedDataSource(ProcessedTables.PROPERTIES, Some(testDate), propertiesDF)
    
    // When: Execute the ETL
    val etl = ApartmentInvestmentAnalysis(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result (for now it just returns wallapop properties)
    result.count() shouldBe 2
    
    val ids = result.select("id").as[String].collect()
    ids should contain allOf("prop1", "prop2")
  }
}
