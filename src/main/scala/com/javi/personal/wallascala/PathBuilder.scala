package com.javi.personal.wallascala

object PathBuilder {

  private val ACCOUNT = "tfgbs"
  private val CONTAINER = "datalake"
  private val V2 = true

  private val locationTemplate = StorageAccountLocation(
    account = ACCOUNT,
    container = CONTAINER,
    path = "",
    v2 = V2
  )

  def buildStagingPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"staging/$source/$datasetName")

  def buildRawPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"raw/$source/$datasetName")

  def buildSanitedPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"sanited/$source/$datasetName")

  def buildExcludedPath(source: String, datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"sanited/excluded/$source/$datasetName")

  def buildProcessedPath(datasetName: String): StorageAccountLocation =
    locationTemplate.copy(path = f"processed/$datasetName")

}
