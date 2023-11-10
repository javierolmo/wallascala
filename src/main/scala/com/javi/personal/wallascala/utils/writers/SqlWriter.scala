package com.javi.personal.wallascala.utils.writers
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class SqlWriter(layer: String, datasetName: String) extends Writer {

  def host(): String = "localhost"
  def port(): Int = 3306
  def user(): String = "root"
  def pass(): String = "1234"
  def saveMode(): SaveMode = SaveMode.Overwrite

  override def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    dataFrame
      .write
      .format("jdbc")
      .mode(saveMode())
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"jdbc:mysql://$host:$port/$layer")
      .option("dbtable", datasetName)
      .option("user", user())
      .option("password", pass())
      .save()
  }

}
