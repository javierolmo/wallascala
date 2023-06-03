package com.javi.personal.wallascala.model.catalog

import com.javi.personal.wallascala.model.services.impl.blob.model.StorageAccountLocation

object DataCatalog {

  val PISO_WALLAPOP: CatalogItem = CatalogItem("piso_wallapop", "piso", 50, "wallapop")
  val PISO_FOTOCASA: CatalogItem = CatalogItem("piso_fotocasa", "piso", 50, "fotocasa")

}

case class CatalogItem(datasetName: String, searchKeywords: String, pages: Int, datasource: String) {

  def stagingLocation: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = f"staging/$datasource/*",
    v2 = true
  )

  def rawLocation: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = f"raw/$datasource/$datasetName",
    v2 = true
  )

  def sanitedLocation: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = f"sanited/$datasetName",
    v2 = true
  )

  def excludedLocation: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = f"sanited/excluded/$datasetName",
    v2 = true
  )

}
