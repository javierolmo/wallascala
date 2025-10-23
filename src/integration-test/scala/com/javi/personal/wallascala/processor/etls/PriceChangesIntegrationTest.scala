package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessedTables, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate

class PriceChangesIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PriceChangesIntegrationTest")
    .getOrCreate()

  "PriceChanges ETL" should "detect price changes between yesterday and today" in {
    // Given: Test dates and config
    val today = LocalDate.of(2024, 1, 15)
    val yesterday = LocalDate.of(2024, 1, 14)
    val config = ProcessorConfig("price_changes", today, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Properties schema
    val propertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("price", IntegerType)
    ))
    
    // Given: Yesterday's properties
    val yesterdayPropertiesData = Seq(
      Row("prop1", 250000),
      Row("prop2", 300000),
      Row("prop3", 180000),
      Row("prop4", 400000)  // This one won't appear today (not in price changes)
    )
    
    val yesterdayPropertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(yesterdayPropertiesData),
      propertiesSchema
    )
    
    // Given: Today's properties
    val todayPropertiesData = Seq(
      Row("prop1", 240000),   // Price decreased from 250000
      Row("prop2", 300000),   // Price stayed the same (should not appear in results)
      Row("prop3", 185000),   // Price increased from 180000
      Row("prop5", 350000)    // New property (not in price changes)
    )
    
    val todayPropertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(todayPropertiesData),
      propertiesSchema
    )
    
    // Register mock data sources
    mockDataSource.registerProcessedDataSource(ProcessedTables.WALLAPOP_PROPERTIES, Some(yesterday), yesterdayPropertiesDF)
    mockDataSource.registerProcessedDataSource(ProcessedTables.WALLAPOP_PROPERTIES, Some(today), todayPropertiesDF)
    
    // When: Execute the ETL
    val etl = PriceChanges(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result
    result.count() shouldBe 2  // Only prop1 and prop3 changed price
    
    val resultSeq = result.collect().toSeq
    
    // Verify prop1 (price decreased)
    val prop1 = resultSeq.find(_.getAs[String]("id") == "prop1").get
    prop1.getAs[Int]("previous_price") shouldBe 250000
    prop1.getAs[Int]("new_price") shouldBe 240000
    prop1.getAs[Double]("discount") shouldBe -10000.0
    prop1.getAs[Double]("discount_rate") shouldBe -0.04  // -4%
    
    // Verify prop3 (price increased)
    val prop3 = resultSeq.find(_.getAs[String]("id") == "prop3").get
    prop3.getAs[Int]("previous_price") shouldBe 180000
    prop3.getAs[Int]("new_price") shouldBe 185000
    prop3.getAs[Double]("discount") shouldBe 5000.0
    prop3.getAs[Double]("discount_rate") shouldBe 0.0278  // ~2.78%
  }
}
