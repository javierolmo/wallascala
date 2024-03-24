package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * SparkWriter is an abstract class that defines the basic methods to write a DataFrame.
 * @param database The database of the DataFrame to write.
 * @param table The table of the DataFrame to write.
 * @param format The format of the DataFrame to write.
 * @param saveMode The save mode of the DataFrame to write.
 * @param options The options of the DataFrame to write.
 * @param spark The SparkSession to use.
 */
case class SparkSqlWriter
  (database: String, table: String, databaseConnection: DatabaseConnection = DatabaseConnection.fromEnv(), format: String = "jdbc", saveMode: String = "overwrite", options: Map[String, String] = Map())
  (implicit spark: SparkSession)
extends SparkWriter(format=format, saveMode=saveMode, options=options) {

  /**
   * This method writes a DataFrame.
   * @param dataFrame The DataFrame to write.
   * @param spark The SparkSession to use.
   */
  override def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    baseWriter(dataFrame)
      // .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", s"jdbc:sqlserver://${databaseConnection.host}:${databaseConnection.port};database=$database")
      .option("dbtable", table)
      .option("user", databaseConnection.user)
      .option("password", databaseConnection.pass)
      .save()
  }

}
