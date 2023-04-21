package com.javi.personal.wallascala.controller

import com.javi.personal.wallascala.extractor.Extractor
import com.javi.personal.wallascala.extractor.wallapop.WallapopApiExtractor
import com.javi.personal.wallascala.services.{BlobService, SecretService}

object ExtractController {

  def apply(): ExtractController = {
    val extractors = Seq(
      new WallapopApiExtractor(BlobService(SecretService()))
    )
    new ExtractController(extractors)
  }

}

class ExtractController(extractors: Seq[Extractor]) {

  def extract(): Unit = {
    extractors.foreach(_.extract())
  }

}
