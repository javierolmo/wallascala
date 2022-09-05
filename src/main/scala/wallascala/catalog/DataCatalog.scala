package com.javi.personal
package wallascala.catalog

object DataCatalog {

  val MX_KEYS: CatalogItem = CatalogItem("mxkeys", "mx keys", 50)
  val PLANTA: CatalogItem = CatalogItem("planta", "planta", 50)

}

case class CatalogItem(datasetName: String, searchKeywords: String, pages: Int)
