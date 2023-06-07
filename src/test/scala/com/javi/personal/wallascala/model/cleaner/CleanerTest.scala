package com.javi.personal.wallascala.model.cleaner

import com.javi.personal.wallascala.model.SparkSessionWrapper
import com.javi.personal.wallascala.model.catalog.{CatalogItem, DataCatalog}
import com.javi.personal.wallascala.model.cleaner.model.{CleanerMetadata, CleanerMetadataField}
import com.javi.personal.wallascala.model.services.impl.blob.SparkSessionFactory
import com.javi.personal.wallascala.model.services.impl.blob.model.ReadConfig
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

class CleanerTest extends AnyFlatSpec {

  val spark = SparkSessionFactory.build()

  "Cleaner.fieldCleaner()" should "clean integer field" in {
    val dummyDF = spark.createDataFrame(Seq(
      ("1", "a", "true"),
      ("2", "b", "false"),
      ("abc", "c", "falso")
    )).toDF("integer_type", "string_type", "bool_type")
      .withColumn("integer_type", col("integer_type").cast(StringType))
      .withColumn("string_type", col("string_type").cast(StringType))
      .withColumn("bool_type", col("bool_type").cast(StringType))
    val metadata = CleanerMetadata(
      datasetName = "dummy",
      fields = Seq(
        CleanerMetadataField("integer_type", "Integer"),
        CleanerMetadataField("string_type", "String"),
        CleanerMetadataField("bool_type", "Boolean")
      )
    )

    val dfTransformed = metadata.fields.foldLeft(dummyDF)((df:DataFrame, field:CleanerMetadataField) => {
      df.withColumn(field.name, field.buildUDF.apply(col(field.name)))
    })

    dfTransformed.show()
  }

  it should "clean integer field2" in {
    val blobService = BlobService(SecretService())
    val location = DataCatalog.PISO_WALLAPOP.rawLocation.cd(LocalDate.of(2023, 4, 20))
    val inputDF: DataFrame = blobService.read(location, ReadConfig(format = "parquet"))

    val metadata = CleanerMetadata.findByCatalogItem(DataCatalog.PISO_WALLAPOP).get

    val dfTransformed = metadata.fields.foldLeft(inputDF)((df: DataFrame, field: CleanerMetadataField) => {
      df.withColumn(field.name, field.buildUDF.apply(col(field.name)))
    }).select(metadata.fields.map(field => col(field.name)): _*)

    dfTransformed.show()
  }

  "Cleaner.validate()" should "apply schema to dataframe" in {
    val cleaner: Cleaner = new Cleaner(BlobService(SecretService()))
    val dummyDF = spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("integer_type", "string_type")
    val metadata: CleanerMetadata = CleanerMetadata(
      datasetName = "dummy",
      fields = Seq(
        CleanerMetadataField("integer_type", "Integer"),
        CleanerMetadataField("string_type", "String")
      )
    )

    cleaner.validate(dummyDF, metadata)
  }

}
