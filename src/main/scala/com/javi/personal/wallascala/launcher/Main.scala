package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object Main {

  private implicit lazy val spark: SparkSession = SparkSessionFactory.build()

  def main(args: Array[String]): Unit = {
    val config = LauncherConfig.parse(args)
    Launcher.execute(config)
  }

}
