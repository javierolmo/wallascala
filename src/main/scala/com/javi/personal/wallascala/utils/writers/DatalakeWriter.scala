package com.javi.personal.wallascala.utils.writers

import com.javi.personal.wallascala.{PathBuilder, StorageAccountLocation}
import com.javi.personal.wallascala.utils.{Layer, Processed, Raw, Sanited, SanitedExcluded, Staging}
import org.apache.spark.sql.functions.{col, lpad}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class DatalakeWriter(layer: Layer, table: String, source: String = "") extends Writer {

  def partitionBy(): Seq[String] = Seq("year", "month", "day")
  def saveMode(): SaveMode = SaveMode.Overwrite

  private def location(): StorageAccountLocation = {
    require(layer != null)
    layer match {
      case Processed => PathBuilder.buildProcessedPath(table)
      case Sanited =>
        require(source.nonEmpty)
        PathBuilder.buildSanitedPath(source, table)
      case SanitedExcluded =>
        require(source.nonEmpty)
        PathBuilder.buildExcludedPath(source, table)
      case Staging =>
        require(source.nonEmpty)
        PathBuilder.buildStagingPath(source, table)
      case Raw =>
        require(source.nonEmpty)
        PathBuilder.buildRawPath(source, table)
    }
  }

  private def hiveTableName(): String =
    if(source.isEmpty) table
    else s"${source}_$table"

  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    val formatYear = if(dataFrame.columns.contains("year")) dataFrame.withColumn("year", lpad(col("year"), 4, "0")) else dataFrame
    val formatMonth = if(formatYear.columns.contains("month")) formatYear.withColumn("month", lpad(col("month"), 2, "0")) else formatYear
    val formatDay = if(formatMonth.columns.contains("day")) formatMonth.withColumn("day", lpad(col("day"), 2, "0")) else formatMonth
    formatDay.write
      .mode(saveMode())
      .format("parquet")
      .partitionBy(partitionBy():_*)
      .option("path", location().url)
      .saveAsTable(f"$layer.${hiveTableName()}")
  }

}
