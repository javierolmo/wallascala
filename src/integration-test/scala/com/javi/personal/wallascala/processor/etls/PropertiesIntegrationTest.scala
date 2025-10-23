package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{MockDataSourceProvider, ProcessorConfig}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class PropertiesIntegrationTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PropertiesIntegrationTest")
    .getOrCreate()

  import spark.implicits._

  "Properties ETL" should "union properties from wallapop, pisos, and fotocasa" in {
    // Given: Test date and config
    val testDate = LocalDate.of(2024, 1, 15)
    val config = ProcessorConfig("properties", testDate, "/tmp/test-output")
    
    // Given: Mock data source provider
    val mockDataSource = new MockDataSourceProvider()
    
    // Given: Wallapop properties data
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
    
    val wallapopData = Seq(
      Row("wallapop1", "Piso Madrid", 250000, 80, 2, 1, "piso-madrid", 
          "2024-01-01", "EUR", true, false, false, "Madrid", "ES", 28001, 
          "2024-01-10", "sale", false, "Bonito piso", false, "flat")
    )
    
    val wallapopDF = spark.createDataFrame(
      spark.sparkContext.parallelize(wallapopData),
      wallapopPropertiesSchema
    )
    
    // Given: Provinces data
    val provincesSchema = StructType(Array(
      StructField("codigo", StringType),
      StructField("provincia", StringType),
      StructField("ccaa", StringType)
    ))
    
    val provincesData = Seq(
      Row("28", "Madrid", "Comunidad de Madrid")
    )
    
    val provincesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(provincesData),
      provincesSchema
    )
    
    // Given: Pisos properties data
    val pisosSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("size", IntegerType),
      StructField("rooms", IntegerType),
      StructField("bathrooms", IntegerType),
      StructField("url", StringType),
      StructField("city", StringType),
      StructField("operation", StringType),
      StructField("description", StringType),
      StructField("type", StringType)
    ))
    
    val pisosData = Seq(
      Row("pisos1", "Apartamento Valencia", 180000, 70, 2, 1, "/apartamento-valencia", 
          "Valencia", "sale", "Apartamento moderno", "flat")
    )
    
    val pisosDF = spark.createDataFrame(
      spark.sparkContext.parallelize(pisosData),
      pisosSchema
    )
    
    // Given: Fotocasa properties data (empty for this test)
    val fotocasaSchema = StructType(Array(
      StructField("id", StringType),
      StructField("title", StringType),
      StructField("price", IntegerType),
      StructField("features__size", IntegerType),
      StructField("features__rooms", IntegerType),
      StructField("features__bathrooms", IntegerType),
      StructField("url", StringType),
      StructField("city", StringType),
      StructField("operation", StringType),
      StructField("type", StringType)
    ))
    
    val fotocasaData = Seq(
      Row("fotocasa1", "Casa Barcelona", 400000, 150, 4, 2, "/casa-barcelona", 
          "Barcelona", "sale", "house")
    )
    
    val fotocasaDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fotocasaData),
      fotocasaSchema
    )
    
    // Register mock data sources
    mockDataSource.registerSanitedDataSource("wallapop", "properties", Some(testDate), wallapopDF)
    mockDataSource.registerSanitedDataSource("opendatasoft", "provincias-espanolas", None, provincesDF)
    mockDataSource.registerSanitedDataSource("pisos", "properties", Some(testDate), pisosDF)
    mockDataSource.registerSanitedDataSource("fotocasa", "properties", Some(testDate), fotocasaDF)
    
    // When: Execute the ETL
    val etl = new Properties(config, mockDataSource)
    val result = etl.buildForTesting()
    
    // Then: Verify the result
    result.count() shouldBe 3  // 1 from wallapop, 1 from pisos, 1 from fotocasa
    
    // Verify wallapop property
    val wallapopProp = result.filter($"id" === "wallapop1")
    wallapopProp.count() shouldBe 1
    wallapopProp.select("source").as[String].collect().head shouldBe "wallapop"
    wallapopProp.select("city").as[String].collect().head shouldBe "Madrid"
    
    // Verify pisos property
    val pisosProp = result.filter($"id" === "pisos1")
    pisosProp.count() shouldBe 1
    pisosProp.select("source").as[String].collect().head shouldBe "pisos"
    pisosProp.select("city").as[String].collect().head shouldBe "Valencia"
    val pisosLink = pisosProp.select("link").as[String].collect().head
    pisosLink should startWith("https://www.pisos.com")
    
    // Verify fotocasa property
    val fotocasaProp = result.filter($"id" === "fotocasa1")
    fotocasaProp.count() shouldBe 1
    fotocasaProp.select("source").as[String].collect().head shouldBe "fotocasa"
    fotocasaProp.select("city").as[String].collect().head shouldBe "Barcelona"
    val fotocasaLink = fotocasaProp.select("link").as[String].collect().head
    fotocasaLink should startWith("https://www.fotocasa.es")
  }
}
