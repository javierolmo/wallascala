package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionFactory.build()
    val config = LauncherConfig.parse(args)
    Launcher.execute(config)
    spark.stop()
  }

}
