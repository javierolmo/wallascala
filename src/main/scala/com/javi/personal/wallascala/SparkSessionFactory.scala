package com.javi.personal.wallascala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(extraConf: Seq[(String, String)] = Seq()): SparkSession = {
    val builder = SparkSession.builder()
      .config(sparkConf(extraConf))
      .appName("wallascala")
      .applyIf(!runsInCluster)(_.master("local[*]"))

    val spark = builder.getOrCreate()
    initializeDatabases(spark)
    spark
  }

  private implicit class BuilderOps(builder: SparkSession.Builder) {
    def applyIf(condition: Boolean)(f: SparkSession.Builder => SparkSession.Builder): SparkSession.Builder =
      if (condition) f(builder) else builder
  }

  private def sparkConf(extraConf: Seq[(String, String)] = Seq()): SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    conf.set("spark.sql.ansi.enabled", "false")
    conf.set("fs.azure.account.key.tfgbs.blob.core.windows.net", readEnvironmentVariable("TFGBS_KEY"))
    conf.set("fs.azure.account.key.tfgbs.dfs.core.windows.net", readEnvironmentVariable("TFGBS_KEY"))
    extraConf.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  private def runsInCluster: Boolean = sys.env.contains("MASTER")

  private def initializeDatabases(spark: SparkSession): Unit = 
    Seq("raw", "sanited", "excluded", "processed")
      .foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

  private def readEnvironmentVariable(variableName: String): String =
    sys.env.getOrElse(variableName, throw new RuntimeException(s"Environment variable $variableName not found"))

}
