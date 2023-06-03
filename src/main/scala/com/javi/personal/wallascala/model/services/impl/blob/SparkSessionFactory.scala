package com.javi.personal.wallascala.model.services.impl.blob

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build() = {
    val builder = SparkSession.builder()
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .config("spark.mongodb.write.connection.uri", "mongodb+srv://spark:47921093mM?@gold-pisos.aqkuvzq.mongodb.net/gold-pisos.gold")
      .appName("lib_pulse_druid")
    val builderWithMaster = if (runsInCluster) builder else builder.master("local[*]")
    builderWithMaster.getOrCreate()
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

}
