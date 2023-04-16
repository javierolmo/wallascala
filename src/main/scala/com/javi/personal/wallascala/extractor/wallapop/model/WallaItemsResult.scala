package com.javi.personal.wallascala.extractor.wallapop.model

case class WallaItemsResult(
                             search_objects: Seq[WallaItem] = Nil,
                             from: Int = 0,
                             to: Int = 0,
                             distance_ordered: Boolean = false,
                             keywords: String = "",
                             order: String = "",
                             search_point: WallaLocation = null
                           )
