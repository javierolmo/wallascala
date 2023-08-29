package com.javi.personal.wallascala.utils

import com.javi.personal.wallascala.{SparkSessionFactory, SparkUtils}
import com.nimbusds.jose.util.StandardCharset
import org.apache.spark.sql.DataFrame
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
  implicit val spark = SparkSessionFactory.build()
  def main(args: Array[String]): Unit = {
    val df = readProcessed("price_changes").filter(ymdCondition(LocalDate.of(2023, 8, 29)))
      .filter(col("city") === "Vigo")
      .select("city", "title", "previous_price", "new_price", "link")

    HtmlReport()
      .withTable("Cambio de precios", "Modificaciones en el precio de inmuebles el día 29/08/2023", df)
      .write().disk()

  }
}


