package com.javi.personal.wallascala.utils

import java.nio.file.Path

case class TelegramApi(botToken: String) {

  private val baseUrl = s"https://api.telegram.org/bot$botToken"

  def sendMessage(message: String, to: String): Unit = {
    val sendMessageEndpoint = baseUrl.path("sendMessage")
    val sendMessageRequest = SendMessageRequest(to, message)
    val requestBody = sendMessageRequest.asJson.noSpaces

    val request = basicRequest
      .post(sendMessageEndpoint)
      .body(requestBody)
      .contentType("application/json")

    val response = request.send()

    // Handle the response if needed
  }

  def sendDocument(document: Path, to: String): Unit = {
    val sendDocumentEndpoint = baseUrl.path("sendDocument")
    val multipart = multipartFile("document", document.toFile)
    val formData = Map("chat_id" -> Seq(to))

    val request = basicRequest
      .post(sendDocumentEndpoint)
      .multipartBody(multipart)
      .params(formData)

    val response = request.send()

    // Handle the response if needed
  }
}

object TelegramApi {
  def apply(botToken: String): TelegramApi = new TelegramApi(botToken)
}

}

object TelegramApi {

}
