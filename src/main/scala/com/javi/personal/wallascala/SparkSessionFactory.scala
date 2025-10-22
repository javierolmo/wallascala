package com.javi.personal.wallascala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(extraConf: Seq[(String, String)] = Seq()): SparkSession = {
    val tfgbsKey = readEnvironmentVariable("TFGBS_KEY")
    val runsInCluster = sys.env.contains("MASTER")
    build(tfgbsKey, runsInCluster, extraConf)
  }

  def build(tfgbsKey: String, runsInCluster: Boolean, extraConf: Seq[(String, String)]): SparkSession = {
    val builder = SparkSession.builder()
      .config(sparkConf(tfgbsKey, extraConf))
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

  private def sparkConf(tfgbsKey: String, extraConf: Seq[(String, String)]): SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    conf.set("fs.azure.account.key.tfgbs.blob.core.windows.net", tfgbsKey)
    conf.set("fs.azure.account.key.tfgbs.dfs.core.windows.net", tfgbsKey)
    extraConf.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  private def initializeDatabases(spark: SparkSession): Unit = 
    Seq("raw", "sanited", "excluded", "processed")
      .foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

  private def readEnvironmentVariable(variableName: String): String =
    sys.env.getOrElse(variableName, throw new RuntimeException(s"Environment variable $variableName not found"))

}
