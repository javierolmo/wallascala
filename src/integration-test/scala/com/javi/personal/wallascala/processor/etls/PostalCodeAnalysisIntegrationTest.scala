package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessedTables, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class PostalCodeAnalysisIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PostalCodeAnalysisIntegrationTest")
    .getOrCreate()

  "PostalCodeAnalysis ETL" should "aggregate properties by postal code, type, and operation" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("postal_code_analysis", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Sample properties data (using StringType for dates)
    val propertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("city", StringType),
      StructField("postal_code", IntegerType),
      StructField("type", StringType),
      StructField("operation", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("date", StringType)
    ))
    
    val propertiesData = Seq(
      // Madrid postal code 28001, flats for sale
      Row("prop1", "Madrid", 28001, "flat", "sale", 250000, 80, "2024-01-01"),
      Row("prop2", "Madrid", 28001, "flat", "sale", 300000, 100, "2024-01-05"),
      Row("prop3", "Madrid", 28001, "flat", "sale", 200000, 70, "2024-01-10"),
      Row("prop3", "Madrid", 28001, "flat", "sale", 220000, 70, "2024-01-08"), // Old version of prop3 - same ID
      
      // Madrid postal code 28001, flats for rent
      Row("prop4", "Madrid", 28001, "flat", "rent", 1500, 80, "2024-01-12"),
      
      // Barcelona postal code 8001, flats for sale
      Row("prop5", "Barcelona", 8001, "flat", "sale", 350000, 90, "2024-01-14"),
      Row("prop6", "Barcelona", 8001, "flat", "sale", 400000, 110, "2024-01-15"),
      
      // Property with zero values (should be excluded in price_m2 calculation)
      Row("prop7", "Madrid", 28002, "flat", "sale", 0, 0, "2024-01-15")
    )
    
    val propertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(propertiesData),
      propertiesSchema
    )
    
    // Register mock data source
    mockDataSource.registerProcessedDataSource(ProcessedTables.PROPERTIES, None, propertiesDF)
    
    // When: Execute the ETL
    val etl = PostalCodeAnalysis(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result
    val resultSeq = result.collect().toSeq
    
    // We should have 4 groups: 
    // - Madrid 28001 flat sale (3 properties after deduplication)
    // - Madrid 28001 flat rent (1 property)
    // - Barcelona 8001 flat sale (2 properties)
    // - Madrid 28002 flat sale (1 property with zeros)
    resultSeq.length shouldBe 4
    
    // Verify Madrid 28001 flats for sale
    val madrid28001Sale = resultSeq.find(row => 
      row.getAs[Int]("postal_code") == 28001 && 
      row.getAs[String]("operation") == "sale"
    ).get
    
    madrid28001Sale.getAs[String]("city") shouldBe "Madrid"
    madrid28001Sale.getAs[Long]("count") shouldBe 3  // prop1, prop2, prop3 (latest version)
    madrid28001Sale.getAs[Double]("average_price") shouldBe 250000.0  // (250000 + 300000 + 200000) / 3
    madrid28001Sale.getAs[Double]("average_surface") shouldBe 83.33  // (80 + 100 + 70) / 3, rounded to 2 decimals
    madrid28001Sale.getAs[Double]("average_price_m2") should be > 2500.0
    madrid28001Sale.getAs[Double]("average_price_m2") should be < 3200.0
    
    // Verify Barcelona 8001 flats for sale
    val barcelona8001Sale = resultSeq.find(row => 
      row.getAs[Int]("postal_code") == 8001 && 
      row.getAs[String]("operation") == "sale"
    ).get
    
    barcelona8001Sale.getAs[String]("city") shouldBe "Barcelona"
    barcelona8001Sale.getAs[Long]("count") shouldBe 2
    barcelona8001Sale.getAs[Double]("average_price") shouldBe 375000.0  // (350000 + 400000) / 2
    barcelona8001Sale.getAs[Double]("average_surface") shouldBe 100.0  // (90 + 110) / 2
  }
}
