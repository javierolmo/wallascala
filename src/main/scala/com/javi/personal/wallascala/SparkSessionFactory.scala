package com.javi.personal.wallascala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .config(sparkConf())
      .appName("wallascala")

    val builderWithMaster = if (runsInCluster) builder else builder.master("local[*]")
    val spark = builderWithMaster.getOrCreate()
    initializeDatabases(spark)
    spark
  }

  private def sparkConf(): SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    conf.set("fs.azure.account.key.tfgbs.blob.core.windows.net", keyFromEnv("TFGBS_KEY"))
    conf.set("fs.azure.account.key.tfgbs.dfs.core.windows.net", keyFromEnv("TFGBS_KEY"))
    conf
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

  private def initializeDatabases(spark: SparkSession): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS raw")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS sanited")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS excluded")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS processed")
  }

  private def keyFromEnv(env: String): String =
    sys.env.getOrElse(env, throw new RuntimeException(s"Environment variable $env not found"))

}
