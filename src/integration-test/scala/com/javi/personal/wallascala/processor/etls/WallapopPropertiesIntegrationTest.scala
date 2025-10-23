package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class WallapopPropertiesIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("WallapopPropertiesIntegrationTest")
    .getOrCreate()

  import spark.implicits._

  "WallapopProperties ETL" should "transform wallapop properties and join with provinces" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("wallapop_properties", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Sample wallapop properties data
    val wallapopPropertiesSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("surface", IntegerType),
      StructField("rooms", IntegerType),
      StructField("bathrooms", IntegerType),
      StructField("web_slug", StringType),
      StructField("creation_date", StringType),
      StructField("currency", StringType),
      StructField("elevator", BooleanType),
      StructField("garage", BooleanType),
      StructField("garden", BooleanType),
      StructField("location__city", StringType),
      StructField("location__country_code", StringType),
      StructField("location__postal_code", IntegerType),
      StructField("modification_date", StringType),
      StructField("operation", StringType),
      StructField("pool", BooleanType),
      StructField("storytelling", StringType),
      StructField("terrace", BooleanType),
      StructField("type", StringType)
    ))
    
    val wallapopPropertiesData = Seq(
      Row("prop1", "Piso en Madrid Centro", 250000, 80, 2, 1, "piso-madrid-centro", 
          "2024-01-01", "EUR", true, false, false, "Madrid", "ES", 28001, 
          "2024-01-10", "sale", false, "Bonito piso en el centro", false, "flat"),
      Row("prop2", "Apartamento en Barcelona", 300000, 100, 3, 2, "apartamento-barcelona", 
          "2024-01-05", "EUR", false, true, false, "Barcelona", "ES", 8001, 
          "2024-01-12", "sale", true, "Gran apartamento", true, "flat")
    )
    
    val wallapopPropertiesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(wallapopPropertiesData),
      wallapopPropertiesSchema
    )
    
    // Given: Sample provinces data
    val provincesSchema = StructType(Array(
      StructField("codigo", StringType),
      StructField("provincia", StringType),
      StructField("ccaa", StringType)
    ))
    
    val provincesData = Seq(
      Row("28", "Madrid", "Comunidad de Madrid"),
      Row("08", "Barcelona", "Cataluña")
    )
    
    val provincesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(provincesData),
      provincesSchema
    )
    
    // Register mock data sources
    mockDataSource.registerSanitedDataSource("wallapop", "properties", Some(testDate), wallapopPropertiesDF)
    mockDataSource.registerSanitedDataSource("opendatasoft", "provincias-espanolas", None, provincesDF)
    
    // When: Execute the ETL
    val etl = new WallapopProperties(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result
    result.count() shouldBe 2
    
    // Verify all results have the expected source and date
    result.filter($"source" === "wallapop").count() shouldBe 2
    result.filter($"date" === "2024-01-15").count() shouldBe 2
    result.filter($"link".startsWith("https://es.wallapop.com/item/")).count() shouldBe 2
    
    // Verify Madrid property
    val madridProp = result.filter($"id" === "prop1")
    madridProp.count() shouldBe 1
    madridProp.select("city").as[String].collect().head shouldBe "Madrid"
    madridProp.select("province").as[String].collect().head shouldBe "Madrid"
    madridProp.select("region").as[String].collect().head shouldBe "Comunidad de Madrid"
    madridProp.select("postal_code").as[Int].collect().head shouldBe 28001
    
    // Verify Barcelona property
    val barcelonaProp = result.filter($"id" === "prop2")
    barcelonaProp.count() shouldBe 1
    barcelonaProp.select("city").as[String].collect().head shouldBe "Barcelona"
    barcelonaProp.select("province").as[String].collect().head shouldBe "Barcelona"
    barcelonaProp.select("region").as[String].collect().head shouldBe "Cataluña"
    barcelonaProp.select("postal_code").as[Int].collect().head shouldBe 8001
  }
}
