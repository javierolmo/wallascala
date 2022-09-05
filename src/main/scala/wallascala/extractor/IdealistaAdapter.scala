package com.javi.personal
package wallascala.extractor

import wallascala.extractor.BrawlApiAdapter.{Host, Protocol, logger, readToken}
import wallascala.extractor.model.{BrawlStarsResult, Brawler}

import play.api.libs.json.{JsResult, JsValue, Json}
import scalaj.http.Http

object IdealistaAdapter {

  def getPagina(pagina: Int) = {

    val url = pagina match {
      case 1 => "https://www.idealista.com/venta-viviendas/vigo-pontevedra"
      case _ => s"https://www.idealista.com/venta-viviendas/vigo-pontevedra/pagina-${pagina}.htm"
    }

    logger.info(s"Performing GET request -> ${url}")

    val response = Http(url).asString

    response

  }

}
