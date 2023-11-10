package com.javi.personal.wallascala.utils.reader
import com.javi.personal.wallascala.PathBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BlobStorageReader(layer: String, datasetName: String) extends Reader {

  def format(): String = "parquet"

  override def read()(implicit spark: SparkSession): DataFrame =
    spark
      .read
      .format(format())
      .load(PathBuilder.buildProcessedPath(datasetName).url)

}
