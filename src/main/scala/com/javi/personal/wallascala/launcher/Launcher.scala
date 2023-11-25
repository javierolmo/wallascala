package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.utils.reader.{SparkFileReader, SparkReader}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkSqlWriter, SparkWriter}
import org.apache.spark.sql.SparkSession

object Launcher {

  private def copyData(reader: SparkReader, writer: SparkWriter)(implicit spark: SparkSession): Unit = {
    val df = reader.read()
    writer.write(df)
  }

  private def buildReader(config: LauncherConfig)(implicit spark: SparkSession): SparkReader = {
    config.sourceFormat match {
      case "jdbc" => ???
      case _ => new SparkFileReader(path=config.sourcePath.get, format=config.sourceFormat)
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

  def execute(config: LauncherConfig)(implicit spark: SparkSession): Unit = {
    val reader = buildReader(config)
    val writer = buildWriter(config)
    copyData(reader, writer)
  }



}
