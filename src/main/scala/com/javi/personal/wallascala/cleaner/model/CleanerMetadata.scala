package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.types._

object CleanerMetadata {

  private val metadata: Seq[CleanerMetadata] = Seq(
    pisoWallapop,
    pisoFotocasa,
    provinciasEspanolas
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
      CleanerMetadataField("creation_date", TimestampType, transform = Some(fromMillis)),
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
      CleanerMetadataField("modification_date", TimestampType, transform = Some(fromMillis)),
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

  private def pisoFotocasa: CleanerMetadata = CleanerMetadata(
    source ="fotocasa",
    datasetName = "piso_fotocasa",
    fields = Seq(
      CleanerMetadataField("id", IntegerType),
      CleanerMetadataField("title", StringType),
      CleanerMetadataField("price", IntegerType),
      CleanerMetadataField("rooms", IntegerType),
      CleanerMetadataField("size", IntegerType),
      CleanerMetadataField("floor", IntegerType),
      CleanerMetadataField("bathrooms", IntegerType),
      CleanerMetadataField("agent", StringType),
      CleanerMetadataField("url", StringType),
      CleanerMetadataField("city", StringType),
      CleanerMetadataField("source", StringType),
      CleanerMetadataField("timeAgo", IntegerType),
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
