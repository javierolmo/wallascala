package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.utils.DataFrameOps._
import com.javi.personal.wallascala.utils.reader.{SparkFileReader, SparkReader}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkSqlWriter, SparkWriter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object Launcher {

  def execute(config: LauncherConfig)(implicit spark: SparkSession): Unit = {
    val reader = buildReader(config)
    val writer = buildWriter(config)
    
    val dataFrame = reader.read()
      .applyIf(config.flattenFields, SparkReader.flattenFields)
      
    val dataFrameWithColumns = config.newColumns
      .map(_.split("="))
      .filter(_.length == 2)
      .foldLeft(dataFrame) { case (df, Array(name, value)) => df.withColumn(name, lit(value)) }
      
    writer.write(dataFrameWithColumns)
  }

  private def buildReader(config: LauncherConfig)(implicit spark: SparkSession): SparkReader = config.sourceFormat match {
    case "jdbc" => throw new UnsupportedOperationException("JDBC source format is not supported yet.")
    case _ => new SparkFileReader(
      path = config.sourcePath.getOrElse(throw new IllegalArgumentException("Source path is required.")), 
      format = config.sourceFormat
    )
  }

  private def buildWriter(config: LauncherConfig)(implicit spark: SparkSession): SparkWriter = config.targetFormat match {
    case "jdbc" =>
      val Array(database, table) = config.targetTable.get.split("\\.")
      SparkSqlWriter(
        database = database,
        table = table,
        format = config.targetFormat,
        saveMode = config.mode.getOrElse("overwrite")
      )
    case _ => SparkFileWriter(
      path = config.targetPath.get,
      hiveTable = config.targetTable,
      format = config.targetFormat,
      coalesce = config.coalesce,
      saveMode = config.mode.getOrElse("overwrite")
    )
  }

}
