package com.javi.personal.wallascala.cleaner.model

object CleanerMetadata {

  private val metadata: Seq[CleanerMetadata] = Seq(
    pisoWallapop,
    pisoFotocasa
  )

  def findByCatalogItem(source: String, datasetName: String): Option[CleanerMetadata] = {
    metadata.find(metadata => metadata.datasetName == datasetName && metadata.source == source)
  }

  // Create metadata for each field of object WallapopRawItem
  private def pisoWallapop: CleanerMetadata = CleanerMetadata(
    source = "wallapop",
    datasetName = "properties",
    fields = Seq(
      CleanerMetadataField("bathrooms", "Int"),
      CleanerMetadataField("category_id", "Int", equalTo = Some(200)),
      CleanerMetadataField("condition", "String"),
      CleanerMetadataField("creation_date", "epoch"),
      CleanerMetadataField("currency", "String"),
      CleanerMetadataField("distance", "Double"),
      CleanerMetadataField("elevator", "Boolean"),
      CleanerMetadataField("favorited", "Boolean"),
      CleanerMetadataField("flags__banned", "Boolean"),
      CleanerMetadataField("flags__expired", "Boolean"),
      CleanerMetadataField("flags__onhold", "Boolean"),
      CleanerMetadataField("flags__pending", "Boolean"),
      CleanerMetadataField("flags__reserved", "Boolean"),
      CleanerMetadataField("flags__sold", "Boolean"),
      CleanerMetadataField("garage", "Boolean"),
      CleanerMetadataField("garden", "Boolean"),
      CleanerMetadataField("id", "String"),
      CleanerMetadataField("images", "String"),
      CleanerMetadataField("location__city", "String"),
      CleanerMetadataField("location__country_code", "String"),
      CleanerMetadataField("location__postal_code", "Int"),
      CleanerMetadataField("modification_date", "epoch"),
      CleanerMetadataField("operation", "String"),
      CleanerMetadataField("pool", "Boolean"),
      CleanerMetadataField("price", "Double"),
      CleanerMetadataField("rooms", "Int"),
      CleanerMetadataField("storytelling", "String"),
      CleanerMetadataField("surface", "Int"),
      CleanerMetadataField("terrace", "Boolean"),
      CleanerMetadataField("title", "String"),
      CleanerMetadataField("type", "String"),
      CleanerMetadataField("user__id", "String"),
      CleanerMetadataField("visibility_flags__boosted", "Boolean"),
      CleanerMetadataField("visibility_flags__bumped", "Boolean"),
      CleanerMetadataField("visibility_flags__country_bumped", "Boolean"),
      CleanerMetadataField("visibility_flags__highlighted", "Boolean"),
      CleanerMetadataField("visibility_flags__urgent", "Boolean"),
      CleanerMetadataField("web_slug", "String"),
      CleanerMetadataField("city", "String"),
      CleanerMetadataField("source", "String"),
      CleanerMetadataField("date", "String"),
    )
  )

  private def pisoFotocasa: CleanerMetadata = CleanerMetadata(
    source ="fotocasa",
    datasetName = "piso_fotocasa",
    fields = Seq(
      CleanerMetadataField("id", "Int"),
      CleanerMetadataField("title", "String"),
      CleanerMetadataField("price", "Int"),
      CleanerMetadataField("rooms", "Int"),
      CleanerMetadataField("size", "Int"),
      CleanerMetadataField("floor", "Int"),
      CleanerMetadataField("bathrooms", "Int"),
      CleanerMetadataField("agent", "String"),
      CleanerMetadataField("url", "String"),
      CleanerMetadataField("city", "String"),
      CleanerMetadataField("source", "String"),
      CleanerMetadataField("timeAgo", "Int"),
    )
  )
}

case class CleanerMetadata(
  source: String,
  datasetName: String,
  fields: Seq[CleanerMetadataField]
)
