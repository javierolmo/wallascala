package com.javi.personal.wallascala

object PathBuilder {

  private val ACCOUNT = "tfgdatalake"
  private val V2 = true

  private val locationTemplate = StorageAccountLocation(
    account = ACCOUNT,
    container = "",
    path = "",
    v2 = V2
  )

  def buildStagingPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"$source/$datasetName", container = "staging")

  def buildBronzePath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"$source/$datasetName", container = "bronze")

  def buildSilverPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"$source/$datasetName", container = "silver")

  def buildSilverExcludedPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"excluded/$source/$datasetName", container = "silver")

  def buildGoldPath(datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"$datasetName", container = "gold")

}
