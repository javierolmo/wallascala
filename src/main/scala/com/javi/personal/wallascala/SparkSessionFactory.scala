package com.javi.personal.wallascala

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config("fs.azure.account.key.tfgbs.dfs.core.windows.net", "6tBTVIQIEc5LSQXRh7m0NN4xAt9AXX9KeFtpkgOIrwfkS3hvbxbF0kesR6x1i9WaTuNZD4vMqo4i+AStRBqL5Q==")
      .config("fs.azure.account.key.tfgbs.blob.core.windows.net", "6tBTVIQIEc5LSQXRh7m0NN4xAt9AXX9KeFtpkgOIrwfkS3hvbxbF0kesR6x1i9WaTuNZD4vMqo4i+AStRBqL5Q==")
      .appName("wallascala")

    val builderWithMaster = if (runsInCluster) builder else builder.master("local[*]")
    val spark = builderWithMaster.getOrCreate()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS raw")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS sanited")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS sanited_excluded")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS processed")
    spark
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

}
