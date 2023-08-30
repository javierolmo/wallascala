package com.javi.personal.wallascala.utils

import scalaj.http.{Http, MultiPart}

object TelegramConnector {

  private val BotId = "6463943903:AAGurJ0phg1OppAcZOzZSP4wWZ_CYPS4-gA"
  private val TelegramURL = "https://api.telegram.org"
  val ChatId = "-819203070"

  def sendMessage(message: String, chatId: String = ChatId): Boolean = {
    val url = s"$TelegramURL/bot$BotId/sendMessage"
    val body = Seq(
      ("chat_id", chatId),
      ("text", message))
    Http(url)
      .postForm(body)
      .execute()
      .is2xx
  }

  def sendDocument(content: String, to: String): Boolean = {
    val url = s"$TelegramURL/bot$BotId/sendDocument"
    Http(url)
      .postMulti(MultiPart("document", "report.html", "mime", content))
      .params(Seq(
        ("chat_id", to)
      ))
      .execute()
      .is2xx
  }

}
