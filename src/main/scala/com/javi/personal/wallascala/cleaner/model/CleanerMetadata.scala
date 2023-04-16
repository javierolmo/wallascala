package com.javi.personal.wallascala.cleaner.model

import com.javi.personal.wallascala.catalog.{CatalogItem, DataCatalog}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType}

object CleanerMetadata {

  private val metadata: Seq[CleanerMetadata] = Seq(
    piso
  )

  def findByCatalogItem(catalogItem: CatalogItem): Option[CleanerMetadata] = {
    metadata.find(_.datasetName == catalogItem.datasetName)
  }


  // Create metadata for each field of object WallapopRawItem
  private def piso: CleanerMetadata = CleanerMetadata(
    datasetName = DataCatalog.PISO.datasetName,
    fields = Seq(
      CleanerMetadataField("id", StringType),
      CleanerMetadataField("title", StringType),
      CleanerMetadataField("description", StringType),
      CleanerMetadataField("distance", DoubleType),
      CleanerMetadataField("images", StringType),
      CleanerMetadataField("user_id", StringType),
      CleanerMetadataField("user_micro_name", StringType),

    )
  )
}

case class CleanerMetadata(
  datasetName: String,
  fields: Seq[CleanerMetadataField]
)
