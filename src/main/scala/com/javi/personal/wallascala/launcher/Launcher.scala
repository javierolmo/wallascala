package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkUtils
import com.javi.personal.wallascala.utils.reader.{SparkFileReader, SparkReader}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkSqlWriter, SparkWriter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object Launcher extends SparkUtils {

  def execute(config: LauncherConfig)(implicit spark: SparkSession): Unit = {
    val (reader, writer) = (buildReader(config), buildWriter(config))
    val dataFrame = reader.read()
      .applyIf(config.flattenFields, SparkReader.flattenFields)
    val dataFrameWithColumns = config.newColumns
      .map(_.split("="))
      .filter(_.length == 2)
      .foldLeft(dataFrame)((df, column) => addColumn(df, column(0), column(1)))
    writer.write(dataFrameWithColumns)
  }

  private def buildReader(config: LauncherConfig)(implicit spark: SparkSession): SparkReader = {
    config.sourceFormat match {
      case "jdbc" => throw new UnsupportedOperationException("JDBC source format is not supported yet.")
      case _ => new SparkFileReader(path = config.sourcePath.getOrElse(throw new IllegalArgumentException("Source path is required.")), format = config.sourceFormat)
    }
  }

  private def buildWriter(config: LauncherConfig)(implicit spark: SparkSession): SparkWriter = {
    config.targetFormat match {
      case "jdbc" =>
        val database = config.targetTable.get.split("\\.")(0)
        val table = config.targetTable.get.split("\\.")(1)
        SparkSqlWriter(database=database, table=table, format=config.targetFormat)
      case _ => SparkFileWriter(path=config.targetPath.get, hiveTable=config.targetTable, format=config.targetFormat, coalesce=config.coalesce)
    }
  }

  private def addColumn(dataFrame: DataFrame, columnName: String, columnValue: String): DataFrame =
    dataFrame.withColumn(columnName, lit(columnValue))

}
