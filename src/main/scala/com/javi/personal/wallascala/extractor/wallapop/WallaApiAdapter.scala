package com.javi.personal
package wallascala.extractor.wallapop

import com.javi.personal.wallascala.WallaScalaException
import com.javi.personal.wallascala.extractor.wallapop.model._
import org.apache.log4j.Logger
import play.api.libs.json.{Format, JsResult, JsValue, Json}
import scalaj.http.Http

import scala.util.{Failure, Success, Try}

object WallaApiAdapter {

  val logger = Logger.getLogger(this.getClass.getName)

  implicit val jsonFormat3: Format[WallaLocation] = Json.using[Json.WithDefaultValues].format[WallaLocation]
  implicit val jsonFormat4: Format[WallaImage] = Json.using[Json.WithDefaultValues].format[WallaImage]
  implicit val jsonFormat5: Format[WallaUser] = Json.using[Json.WithDefaultValues].format[WallaUser]
  implicit val jsonFormat6: Format[WallaFlags] = Json.using[Json.WithDefaultValues].format[WallaFlags]
  implicit val jsonFormat7: Format[WallaVisibilityFlags] = Json.using[Json.WithDefaultValues].format[WallaVisibilityFlags]
  implicit val jsonFormat2: Format[WallaItem] = Json.using[Json.WithDefaultValues].format[WallaItem]
  implicit val jsonFormat: Format[WallaItemsResult] = Json.using[Json.WithDefaultValues].format[WallaItemsResult]

  def searchItems(keywords: String, filtersSource: String = "search_box", longitude: Double = -8.71245,
                  latitude: Double = 42.2314, categoryIds: Int = 0, nextPage: Option[String]): Try[(Seq[WallaItem], Option[String])] = {

    val url = s"https://pro2.wallapop.com/api/v3/general/search"

    val params: Map[String, String] = Map(
      "keywords" -> keywords,
      "filters_source" -> filtersSource,
      "longitude" -> s"${longitude}".replace(',', '.'),
      "latitude" -> s"${latitude}".replace(',', '.'),
    )

    val request = nextPage match {
      case Some(nextPage) => Http(s"${url}?${nextPage}")
      case None => Http(url).params(params)
    }

    logger.info(s"Performing GET request -> ${request.url}")
    val response = request.asString

    response.code match {
      case x if (x <= 200 && x < 300) =>
        val jsonString: JsValue = Json.parse(response.body)
        val optionResult: JsResult[WallaItemsResult] = Json.fromJson[WallaItemsResult](jsonString)
        val items = optionResult.getOrElse(WallaItemsResult()).search_objects
        val nextPage = response.header("X-NextPage")
        Success(items, nextPage)
      case _ =>
        val exception = WallaScalaException(s"Error ${response.code} calling wallapop API: ${response.body}")
        Failure(exception)
    }


  }

}
