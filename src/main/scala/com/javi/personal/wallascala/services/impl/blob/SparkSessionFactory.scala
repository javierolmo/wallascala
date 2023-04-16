package com.javi.personal
package wallascala.services.impl.blob

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build() = {
    val builder = SparkSession.builder()
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .appName("lib_pulse_druid")
    val builderWithMaster = if (runsInCluster) builder else builder.master("local[*]")
    builderWithMaster.getOrCreate()
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

}
