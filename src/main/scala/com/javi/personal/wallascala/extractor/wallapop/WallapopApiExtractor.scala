package com.javi.personal
package wallascala.extractor.wallapop

import com.javi.personal.wallascala.SparkSessionWrapper
import com.javi.personal.wallascala.catalog.{CatalogItem, DataCatalog}
import com.javi.personal.wallascala.extractor.Extractor
import com.javi.personal.wallascala.extractor.wallapop.model.WallaItem
import com.javi.personal.wallascala.services.BlobService

import scala.util.{Failure, Success, Try}

class WallapopApiExtractor(blobService: BlobService) extends Extractor(blobService) with SparkSessionWrapper {

  import spark.implicits._

  def extractItems(catalogItem: CatalogItem) = {

    def callApi(keywords: String = "", pages: Int, nextPage: Option[String] = Option.empty): Try[Seq[WallaItem]] = {
      if (pages < 1) return Success(Nil)
      WallaApiAdapter.searchItems(keywords = keywords, nextPage = nextPage) match {
        case Success((items, next)) =>
          if (items.length == 0)
            Success(Nil)
          else if (next.isEmpty)
            Success(items)
          else {
            callApi(pages = pages - 1, nextPage = next) match {
              case Success(value) => Success(items ++ value)
              case Failure(exception) => Failure(exception)
            }
          }
        case Failure(exception) => Failure(exception)
      }
    }

    callApi(catalogItem.searchKeywords, catalogItem.pages) match {

      case Success(items) => val rawItems = items.toDS().map(_.toRawItem).toDF()

        writeToDatalake(rawItems, catalogItem)

      case Failure(exception) => throw exception

    }


  }

  override def extract(): Unit = {
    extractItems(DataCatalog.PISO)
  }
}
