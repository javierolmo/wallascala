package com.javi.personal.wallascala

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config("fs.azure.account.key.melodiadl.dfs.core.windows.net", "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg==")
      .config("fs.azure.account.key.melodiadl.blob.core.windows.net", "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg==")
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
