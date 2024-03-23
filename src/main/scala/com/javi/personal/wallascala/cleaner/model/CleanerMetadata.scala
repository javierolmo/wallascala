package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, regexp_replace, to_timestamp}
import org.apache.spark.sql.types._

object CleanerMetadata {

  private val metadata: Seq[CleanerMetadata] = Seq(
    pisoWallapop,
    pisoFotocasa,
    provinciasEspanolas,
    pisosProperties
  )

  def findByCatalogItem(source: String, datasetName: String): Option[CleanerMetadata] = {
    metadata.find(metadata => metadata.datasetName == datasetName && metadata.source == source)
  }

  private def pisoWallapop: CleanerMetadata = CleanerMetadata(
    source = "wallapop",
    datasetName = "properties",
    fields = Seq(
      CleanerMetadataField("bathrooms", IntegerType),
      CleanerMetadataField("category_id", IntegerType, equalTo = Some(200)),
      CleanerMetadataField("condition", StringType),
      CleanerMetadataField("creation_date", TimestampType, transform = Some(to_timestamp)),
      CleanerMetadataField("currency", StringType),
      CleanerMetadataField("distance", DoubleType),
      CleanerMetadataField("elevator", BooleanType),
      CleanerMetadataField("favorited", BooleanType),
      CleanerMetadataField("flags__banned", BooleanType),
      CleanerMetadataField("flags__expired", BooleanType),
      CleanerMetadataField("flags__onhold", BooleanType),
      CleanerMetadataField("flags__pending", BooleanType),
      CleanerMetadataField("flags__reserved", BooleanType),
      CleanerMetadataField("flags__sold", BooleanType),
      CleanerMetadataField("garage", BooleanType),
      CleanerMetadataField("garden", BooleanType),
      CleanerMetadataField("id", StringType),
      CleanerMetadataField("images", StringType),
      CleanerMetadataField("location__city", StringType),
      CleanerMetadataField("location__country_code", StringType),
      CleanerMetadataField("location__postal_code", IntegerType),
      CleanerMetadataField("modification_date", TimestampType, transform = Some(to_timestamp)),
      CleanerMetadataField("operation", StringType),
      CleanerMetadataField("pool", BooleanType),
      CleanerMetadataField("price", DoubleType),
      CleanerMetadataField("rooms", IntegerType),
      CleanerMetadataField("storytelling", StringType),
      CleanerMetadataField("surface", IntegerType),
      CleanerMetadataField("terrace", BooleanType),
      CleanerMetadataField("title", StringType),
      CleanerMetadataField("type", StringType),
      CleanerMetadataField("user__id", StringType),
      CleanerMetadataField("visibility_flags__boosted", BooleanType),
      CleanerMetadataField("visibility_flags__bumped", BooleanType),
      CleanerMetadataField("visibility_flags__country_bumped", BooleanType),
      CleanerMetadataField("visibility_flags__highlighted", BooleanType),
      CleanerMetadataField("visibility_flags__urgent", BooleanType),
      CleanerMetadataField("web_slug", StringType),
      CleanerMetadataField("source", StringType),
      CleanerMetadataField("date", StringType),
    )
  )

  private def fromMillis(inputColumn: Column): Column = from_unixtime(inputColumn / 1000)

  private def removeNonNumeric(inputColumn: Column): Column = regexp_replace(inputColumn, "[^0-9]", "")

  private def removeLineBreaks(inputColumn: Column): Column = regexp_replace(inputColumn, "\n", "")

  private def pisoFotocasa: CleanerMetadata = CleanerMetadata(
    source ="fotocasa",
    datasetName = "properties",
    fields = Seq(
      CleanerMetadataField("address", StringType),
      CleanerMetadataField("description", StringType),
      CleanerMetadataField("features__bathrooms", IntegerType),
      CleanerMetadataField("features__floor", IntegerType),
      CleanerMetadataField("features__rooms", IntegerType),
      CleanerMetadataField("features__size", IntegerType),
      CleanerMetadataField("id", IntegerType),
      CleanerMetadataField("price", IntegerType),
      CleanerMetadataField("timeAgo", IntegerType),
      CleanerMetadataField("title", StringType),
      CleanerMetadataField("url", StringType),
      CleanerMetadataField("city", StringType),
      CleanerMetadataField("operation", StringType),
      CleanerMetadataField("type", StringType)
    )
  )

  private def pisosProperties: CleanerMetadata = CleanerMetadata(
    source ="pisos",
    datasetName = "properties",
    fields = Seq(
      CleanerMetadataField("id", StringType),
      CleanerMetadataField("title", StringType, transform = Some(removeLineBreaks)),
      CleanerMetadataField("price", IntegerType, transform = Some(removeNonNumeric)),
      CleanerMetadataField("url", StringType),
      CleanerMetadataField("description", StringType, transform = Some(removeLineBreaks)),
      CleanerMetadataField("address", StringType),
      CleanerMetadataField("rooms", IntegerType, transform = Some(removeNonNumeric)),
      CleanerMetadataField("bathrooms", IntegerType, transform = Some(removeNonNumeric)),
      CleanerMetadataField("size", IntegerType, transform = Some(removeNonNumeric)),
      CleanerMetadataField("floor", IntegerType, transform = Some(removeNonNumeric)),
      CleanerMetadataField("city", StringType),
      CleanerMetadataField("operation", StringType),
      CleanerMetadataField("type", StringType)
    )
  )

  private def provinciasEspanolas: CleanerMetadata = CleanerMetadata(
    source = "opendatasoft",
    datasetName = "provincias-espanolas",
    fields = Seq(
      CleanerMetadataField("ccaa", StringType),
      CleanerMetadataField("cod_ccaa", IntegerType),
      CleanerMetadataField("codigo", IntegerType),
      CleanerMetadataField("geo_point_2d", StructType(Seq(
        StructField("lat", DoubleType),
        StructField("lon", DoubleType)
      ))),
      CleanerMetadataField("geo_shape", StructType(Seq(
        StructField("geometry", StructType(Seq(
          StructField("coordinates", ArrayType(ArrayType(ArrayType(StringType)))),
          StructField("type", DoubleType)
        ))),
        StructField("type", StringType)
      ))),
      CleanerMetadataField("provincia", StringType),
      CleanerMetadataField("texto", StringType),
    )
  )
}

case class CleanerMetadata(
  source: String,
  datasetName: String,
  fields: Seq[CleanerMetadataField]
)
