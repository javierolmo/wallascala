package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class SparkSqlWriter
  (database: String, table: String, format: String = "jdbc", saveMode: String = "overwrite", options: Map[String, String] = Map())
  (implicit spark: SparkSession)
extends SparkWriter(format=format, saveMode=saveMode, options=options) {

  val host: String = "localhost"
  val port: Int = 3306
  val user: String = "root"
  val pass: String = "1234"

  override def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    baseWriter(dataFrame)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://$host:$port/$database")
      .option("dbtable", table)
      .option("user", user)
      .option("password", pass)
      .save()
  }

}
