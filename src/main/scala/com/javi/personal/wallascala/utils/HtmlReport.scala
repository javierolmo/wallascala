package com.javi.personal.wallascala.utils

import com.javi.personal.wallascala.{SparkSessionFactory, SparkUtils}
import com.nimbusds.jose.util.StandardCharset
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.io.Source


case class HtmlReport(tables: Seq[(String, String, DataFrame)] = Seq()) {

  def withTable(name: String, description: String, data: DataFrame): HtmlReport =
    HtmlReport(tables ++ Seq((name, description, data)))

  def write(): ReportWriter = ReportWriter(this)


}

object HtmlReport extends SparkUtils {
  implicit val spark: SparkSession = SparkSessionFactory.build()

  def publishedToday(date: LocalDate): DataFrame = {
    val todayProperties = readProcessed("properties").filter(ymdCondition(date))
    val yesterdayProperties = readProcessed("properties").filter(ymdCondition(date.minusDays(1)))
    todayProperties
      .filter(col("city") === "Vigo")
      .join(yesterdayProperties, Seq("id"), "leftanti")
      .select("city", "title", "type", "operation", "price", "surface", "rooms", "bathrooms", "link")
      .orderBy("type", "operation")
  }

  def priceChanges(date: LocalDate): DataFrame =
    readProcessed("price_changes")
      .filter(ymdCondition(date))
      .filter(col("city") === "Vigo")
      .select("city", "title", "previous_price", "new_price", "link")


  def main(args: Array[String]): Unit = {

    val Today = LocalDate.now()

    HtmlReport()
      .withTable("Cambios de precios", s"Modificaciones en el precio de inmuebles el día $Today", priceChanges(Today))
      .withTable("Nuevos hoy", s"Nuevos inmuebles publicados a día $Today", publishedToday(Today))
      .write().telegramMessage()

  }
}


