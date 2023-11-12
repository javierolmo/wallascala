package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Launcher(config: LauncherConfig) {

  private implicit lazy val spark: SparkSession = SparkSessionFactory.build()

  def run(): Unit = {
    val dataFrame = config.reader.read()
    val dfWithSelect = selectFields(dataFrame, config.select)
    config.writer.write(dfWithSelect)
  }

  private def selectFields(dataFrame: DataFrame, fields: Option[Seq[String]]): DataFrame =
    if (fields.isDefined) dataFrame.select(fields.get.map(field => col(field)):_*)
    else dataFrame

}
