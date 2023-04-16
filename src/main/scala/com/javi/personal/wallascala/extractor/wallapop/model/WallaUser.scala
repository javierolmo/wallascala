package com.javi.personal.wallascala.extractor.wallapop.model

case class WallaUser(
                      id: String,
                      micro_name: String,
                      image: WallaImage,
                      online: Boolean,
                      kind: String,
                    )
