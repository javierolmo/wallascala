package com.javi.personal.wallascala.utils

import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

case class ReportWriter(report: HtmlReport) {

  private def execute(): String = {
    val sampleReport = Source.fromResource("sample-report.html").getLines().mkString("\n")
    sampleReport.replace("[DATA_PLACEHOLDER]", toJson)
  }

  def disk(fileName: String = "new-report.html"): Unit =
    Files.write(Paths.get(fileName), execute().getBytes(StandardCharsets.UTF_8))

  def telegramMessage(): Unit =
    TelegramConnector.sendDocument(execute(), TelegramConnector.ChatId)

  private def toJson: String = {

    def tableToJson(name: String, description: String, dataFrame: DataFrame) =
      s"""
         |  {
         |     "name": "$name",
         |     "description": "$description",
         |     "headers": [${dataFrame.columns.map(col => s"'$col'").mkString(",")}],
         |     "data": [
         |       ${dataFrame.toJSON.collect().mkString(",\n")}
         |      ]
         |  },
         |""".stripMargin

    s"""
       |[
       |   ${report.tables.map(table => tableToJson(table._1, table._2, table._3)).mkString(",\n")}
       |]
       |""".stripMargin

  }

}
