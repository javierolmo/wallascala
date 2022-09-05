package com.javi.personal
package wallascala.extractor

import wallascala.extractor.model.{BrawlStarsResult, Brawler, Paging, Skill, Cursors}
import org.apache.log4j.Logger
import play.api.libs.json.{Format, JsResult, JsValue, Json}
import scalaj.http.Http
import java.net.URL
import java.util.Properties
import scala.io.Source

object BrawlApiAdapter {

  val logger = Logger.getLogger(this.getClass.getName)

  implicit val cursorsFormat: Format[Cursors] = Json.using[Json.WithDefaultValues].format[Cursors]
  implicit val skillFormat: Format[Skill] = Json.using[Json.WithDefaultValues].format[Skill]
  implicit val pagingFormat: Format[Paging] = Json.using[Json.WithDefaultValues].format[Paging]
  implicit val brawlerFormat: Format[Brawler] = Json.using[Json.WithDefaultValues].format[Brawler]
  implicit val brawlstarsResultFormat: Format[BrawlStarsResult[Brawler]] = Json.using[Json.WithDefaultValues].format[BrawlStarsResult[Brawler]]

  val Protocol = "https"
  val Host = "api.brawlstars.com"

  def getBrawlers(): Seq[Brawler] = {

    val endpoint = "v1/brawlers"
    val url = s"$Protocol://${Host}/$endpoint"

    logger.info(s"Performing GET request -> ${url}")

    val response = Http(url)
      .header("Authorization", s"Bearer ${readToken()}")
      .asString

    val jsonString: JsValue = Json.parse(response.body)
    val jsonresult: JsResult[BrawlStarsResult[Brawler]] = Json.fromJson[BrawlStarsResult[Brawler]](jsonString)
    val optionResult: Option[BrawlStarsResult[Brawler]] = jsonresult.asOpt

    optionResult.get.items

  }

  private def readToken(): String = {
    try {
      val properties = new Properties()
      val url: URL = getClass.getResource("/application.properties")
      properties.load(Source.fromURL(url).bufferedReader())
      properties.getProperty("wallascala.extractor.brawl.token")
    } catch {
      case e: Throwable => e.printStackTrace(); sys.exit(1)
    }
  }

}
