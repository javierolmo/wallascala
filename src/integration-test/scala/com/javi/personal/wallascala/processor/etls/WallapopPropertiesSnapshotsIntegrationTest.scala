package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessedTables, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class WallapopPropertiesSnapshotsIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("WallapopPropertiesSnapshotsIntegrationTest")
    .getOrCreate()

  import spark.implicits._

  "WallapopPropertiesSnapshots ETL" should "create snapshots with start and end dates" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("wallapop_properties_snapshots", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Sample wallapop properties data with multiple dates (using StringType for dates)
    val wallapopPropertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("rooms", IntegerType),
      StructField("bathrooms", IntegerType),
      StructField("link", StringType),
      StructField("source", StringType),
      StructField("creation_date", StringType),
      StructField("currency", StringType),
      StructField("elevator", BooleanType),
      StructField("garage", BooleanType),
      StructField("garden", BooleanType),
      StructField("city", StringType),
      StructField("country", StringType),
      StructField("postal_code", IntegerType),
      StructField("province", StringType),
      StructField("region", StringType),
      StructField("modification_date", StringType),
      StructField("operation", StringType),
      StructField("pool", BooleanType),
      StructField("description", StringType),
      StructField("terrace", BooleanType),
      StructField("type", StringType),
      StructField("date", StringType)
    ))
    
    val wallapopPropertiesData = Seq(
      // Property 1: appeared on 2024-01-01, still active (latest date 2024-01-15)
      Row("prop1", "Piso Madrid", 250000, 80, 2, 1, "http://link1", "wallapop",
          "2024-01-01", "EUR", true, false, false, "Madrid", "ES", 28001,
          "Madrid", "Comunidad de Madrid", "2024-01-01", "sale", false,
          "Description 1", false, "flat", "2024-01-01"),
      Row("prop1", "Piso Madrid", 245000, 80, 2, 1, "http://link1", "wallapop",
          "2024-01-01", "EUR", true, false, false, "Madrid", "ES", 28001,
          "Madrid", "Comunidad de Madrid", "2024-01-05", "sale", false,
          "Description 1", false, "flat", "2024-01-10"),
      Row("prop1", "Piso Madrid", 240000, 80, 2, 1, "http://link1", "wallapop",
          "2024-01-01", "EUR", true, false, false, "Madrid", "ES", 28001,
          "Madrid", "Comunidad de Madrid", "2024-01-10", "sale", false,
          "Description 1", false, "flat", "2024-01-15"),
      // Property 2: appeared on 2024-01-05, disappeared on 2024-01-12
      Row("prop2", "Apartamento Barcelona", 300000, 100, 3, 2, "http://link2", "wallapop",
          "2024-01-05", "EUR", false, true, false, "Barcelona", "ES", 8001,
          "Barcelona", "Cataluña", "2024-01-05", "sale", true,
          "Description 2", true, "flat", "2024-01-05"),
      Row("prop2", "Apartamento Barcelona", 295000, 100, 3, 2, "http://link2", "wallapop",
          "2024-01-05", "EUR", false, true, false, "Barcelona", "ES", 8001,
          "Barcelona", "Cataluña", "2024-01-08", "sale", true,
          "Description 2", true, "flat", "2024-01-12")
    )
    
    val wallapopPropertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(wallapopPropertiesData),
      wallapopPropertiesSchema
    )
    
    // Register mock data source
    mockDataSource.registerProcessedDataSource(ProcessedTables.WALLAPOP_PROPERTIES, None, wallapopPropertiesDF)
    
    // When: Execute the ETL
    val etl = new WallapopPropertiesSnapshots(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result
    result.count() shouldBe 2
    
    // Verify property 1 (still active, no end date)
    val prop1 = result.filter($"id" === "prop1")
    prop1.count() shouldBe 1
    prop1.select("start_date").as[String].collect().head shouldBe "2024-01-01"
    // End date should be null for properties still active
    val prop1EndDate = prop1.select("end_date").collect().head.getAs[String](0)
    Option(prop1EndDate) shouldBe None
    prop1.select("price").as[Int].collect().head shouldBe 240000 // Latest price
    
    // Verify property 2 (inactive, has end date)
    val prop2 = result.filter($"id" === "prop2")
    prop2.count() shouldBe 1
    prop2.select("start_date").as[String].collect().head shouldBe "2024-01-05"
    prop2.select("end_date").as[String].collect().head shouldBe "2024-01-12"
    prop2.select("price").as[Int].collect().head shouldBe 295000 // Latest price
  }
}
