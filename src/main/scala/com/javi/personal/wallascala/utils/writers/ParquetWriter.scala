package com.javi.personal.wallascala.utils.writers

import com.javi.personal.wallascala.PathBuilder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class ParquetWriter(layer: String, datasetName: String) extends Writer {

  def partitionBy(): Seq[String] = Seq("year", "month", "day")
  def saveMode(): SaveMode = SaveMode.Overwrite

  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    dataFrame.write
      .mode(saveMode())
      .format("parquet")
      .partitionBy(partitionBy():_*)
      .option("path", PathBuilder.buildProcessedPath(datasetName).url)
      .saveAsTable(f"processed.$datasetName")
  }

}
