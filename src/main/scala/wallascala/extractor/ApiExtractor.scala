package com.javi.personal
package wallascala.extractor

import wallascala.Application
import wallascala.extractor.model.{Brawler, WallaItem}
import wallascala.catalog.CatalogItem

import org.apache.spark.sql.functions.{col, column, concat_ws, lit, regexp_replace}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object ApiExtractor {

  val now: LocalDate = LocalDate.now()

  val spark: SparkSession = Application.spark
  import spark.implicits._

  def extractItems(catalogItem: CatalogItem) = {

    def callApi(keywords: String= "", pages: Int, nextPage: String = ""): Seq[WallaItem] = {

      if (pages < 1) return Nil

      val (items, next) = try {
        WallaApiAdapter.search_items(keywords = keywords, strParameters = nextPage)
      } catch {
        case _: Throwable => (Nil, "")
      }

      if (items.length == 0) return Nil

      items ++ callApi(pages = pages-1, nextPage = next)

    }

    val items = callApi(catalogItem.searchKeywords, catalogItem.pages)


    val items_df = items.toDF()
      .withColumn("description", regexp_replace(col("description"), "\"", ""))
      .withColumn("reserved", col("flags").getField("reserved"))
      .withColumn("sold", col("flags").getField("sold"))
      .withColumn("pending", col("flags").getField("pending"))
      .withColumn("banned", col("flags").getField("banned"))
      .withColumn("expired", col("flags").getField("expired"))
      .withColumn("onhold", col("flags").getField("onhold"))
      .withColumn("seller_name", col("user").getField("micro_name"))
      .withColumn("seller_image", col("user").getField("image").getField("original"))
      .withColumn("seller_online", col("user").getField("online"))
      .withColumn("seller_kind", col("user").getField("kind"))
      .withColumn("bumped", col("visibility_flags").getField("bumped"))
      .withColumn("highlighted", col("visibility_flags").getField("highlighted"))
      .withColumn("urgent", col("visibility_flags").getField("urgent"))
      .withColumn("country_bumped", col("visibility_flags").getField("country_bumped"))
      .withColumn("boosted", col("visibility_flags").getField("boosted"))
      .withColumn("keywords", lit(catalogItem.searchKeywords))
      .withColumn("year", lit(now.getYear))
      .withColumn("month", lit(now.getMonthValue))
      .withColumn("day", lit(now.getDayOfMonth))
      .drop("images", "user", "flags", "visibility_flags")
      .coalesce(1)

    items_df.write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .save(s"data/wallapop/raw/${catalogItem.datasetName}")

  }

  def extractBrawlers(): Unit = {

    import spark.implicits._

    val brawlers: Seq[Brawler] = BrawlApiAdapter.getBrawlers()

    val brawlersDF: DataFrame = brawlers.toDF()
      .withColumn("year", lit(now.getYear))
      .withColumn("month", lit(now.getMonthValue))
      .withColumn("day", lit(now.getDayOfMonth))
      .coalesce(1)

    brawlersDF.write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .save(s"data/brawlstars/raw/brawlers")


  }

  def extractFlats() = {
    val html = IdealistaAdapter.getPagina(2)
  }

}
