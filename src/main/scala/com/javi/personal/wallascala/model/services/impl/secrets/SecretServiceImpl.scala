package com.javi.personal.wallascala.model.services.impl.secrets

import com.javi.personal.wallascala.model.services.SecretService
import org.apache.log4j.LogManager

class SecretServiceImpl() extends SecretService {

  private val log = LogManager.getLogger(this.getClass)

  override def getSecret(secretName: String): String = {
    secretName match {
      case "melodiadl-key" => "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg=="
      case _ => throw new RuntimeException(s"Secret $secretName not found")
    }
  }
}
