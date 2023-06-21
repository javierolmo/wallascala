package com.javi.personal.wallascala

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build() = {
    val builder = SparkSession.builder()
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config("fs.azure.account.key.melodiadl.dfs.core.windows.net", "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg==")
      .config("fs.azure.account.key.melodiadl.blob.core.windows.net", "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg==")
      .appName("lib_pulse_druid")

    val builderWithMaster = if (runsInCluster) builder else builder.master("local[*]")
    builderWithMaster.getOrCreate()
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

}
