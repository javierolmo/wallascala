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

  "WallapopPropertiesSnapshots ETL" should "get the latest record for each property" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("wallapop_properties_snapshots", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Sample raw wallapop properties data with multiple versions per property
    val wallapopPropertiesRawSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price__amount", IntegerType),
      StructField("type_attributes__surface", IntegerType),
      StructField("type_attributes__rooms", IntegerType),
      StructField("type_attributes__bathrooms", IntegerType),
      StructField("price__currency", StringType),
      StructField("location__city", StringType),
      StructField("location__country_code", StringType),
      StructField("location__postal_code", IntegerType),
      StructField("location__region", StringType),
      StructField("type_attributes__operation", StringType),
      StructField("type_attributes__type", StringType),
      StructField("description", StringType),
      StructField("modified_at", StringType),
      StructField("web_slug", StringType)
    ))

    val wallapopPropertiesRawData = Seq(
      // Property 1: multiple versions, latest should have price 240000 and modification_date 2024-01-10
      Row("prop1", "Piso Madrid", 250000, 80, 2, 1, "EUR", "Madrid", "ES", 28001,
          "Comunidad de Madrid", "sale", "flat", "Description 1", "2024-01-01", "piso-madrid-1"),
      Row("prop1", "Piso Madrid", 245000, 80, 2, 1, "EUR", "Madrid", "ES", 28001,
          "Comunidad de Madrid", "sale", "flat", "Description 1", "2024-01-05", "piso-madrid-1"),
      Row("prop1", "Piso Madrid", 240000, 80, 2, 1, "EUR", "Madrid", "ES", 28001,
          "Comunidad de Madrid", "sale", "flat", "Description 1", "2024-01-10", "piso-madrid-1"),
      // Property 2: two versions, latest should have price 295000 and modification_date 2024-01-08
      Row("prop2", "Apartamento Barcelona", 300000, 100, 3, 2, "EUR", "Barcelona", "ES", 8001,
          "Cataluña", "sale", "flat", "Description 2", "2024-01-05", "apartamento-barcelona-1"),
      Row("prop2", "Apartamento Barcelona", 295000, 100, 3, 2, "EUR", "Barcelona", "ES", 8001,
          "Cataluña", "sale", "flat", "Description 2", "2024-01-08", "apartamento-barcelona-1")
    )

    val wallapopPropertiesRawDF = spark.createDataFrame(
      spark.sparkContext.parallelize(wallapopPropertiesRawData),
      wallapopPropertiesRawSchema
    )

    // Given: Mock provincias data
    val provinciasSchema = StructType(Array(
      StructField("codigo", IntegerType),
      StructField("provincia", StringType),
      StructField("ccaa", StringType)
    ))
    
    val provinciasData = Seq(
      Row(28, "Madrid", "Comunidad de Madrid"),
      Row(8, "Barcelona", "Cataluña")
    )
    
    val provinciasDF = spark.createDataFrame(
      spark.sparkContext.parallelize(provinciasData),
      provinciasSchema
    )
    
    // Register mock data sources
    mockDataSource.registerSanitedDataSource("wallapop", "properties", Some(testDate), wallapopPropertiesRawDF)
    mockDataSource.registerSanitedDataSource("opendatasoft", "provincias-espanolas", None, provinciasDF)

    // When: Execute the ETL
    val etl = new WallapopPropertiesSnapshots(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result has 2 records (one per property - the latest for each)
    result.count() shouldBe 2
    
    // Verify property 1: should have the latest price 240000
    val prop1 = result.filter($"id" === "prop1")
    prop1.count() shouldBe 1
    prop1.select("price").as[Int].collect().head shouldBe 240000
    prop1.select("title").as[String].collect().head shouldBe "Piso Madrid"

    // Verify property 2: should have the latest price 295000
    val prop2 = result.filter($"id" === "prop2")
    prop2.count() shouldBe 1
    prop2.select("price").as[Int].collect().head shouldBe 295000
    prop2.select("title").as[String].collect().head shouldBe "Apartamento Barcelona"
  }
}
