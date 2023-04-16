package com.javi.personal
package wallascala.catalog

import com.javi.personal.wallascala.services.impl.blob.model.StorageAccountLocation

object DataCatalog {

  val MX_KEYS: CatalogItem = CatalogItem("mxkeys", "mx keys", 50, "wallapop")
  val PLANTA: CatalogItem = CatalogItem("planta", "planta", 50, "wallapop")
  val PISO: CatalogItem = CatalogItem("piso", "piso", 50, "wallapop")

}

case class CatalogItem(datasetName: String, searchKeywords: String, pages: Int, datasource: String) {

  def rawLocation: StorageAccountLocation = StorageAccountLocation(
    account = "melodiadl",
    container = "test",
    path = f"raw/$datasetName",
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
