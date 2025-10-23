package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkUtils
import com.javi.personal.wallascala.utils.reader.{SparkFileReader, SparkReader}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkSqlWriter, SparkWriter}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object Launcher extends SparkUtils with com.javi.personal.wallascala.Logging {

  def execute(config: LauncherConfig)(implicit spark: SparkSession): Unit = {
    logger.info("Starting launcher execution")
    val reader = buildReader(config)
    val writer = buildWriter(config)
    
    logger.info("Reading data from source")
    val dataFrame = reader.read()
      .applyIf(config.flattenFields, SparkReader.flattenFields)
    logger.info("Data read successfully, row count: {}", dataFrame.count())
      
    val dataFrameWithColumns = config.newColumns
      .map(_.split("="))
      .filter(_.length == 2)
      .foldLeft(dataFrame) { case (df, Array(name, value)) => df.withColumn(name, lit(value)) }
    
    if (config.newColumns.nonEmpty) {
      logger.debug("Added {} new columns to dataframe", config.newColumns.size)
    }
      
    logger.info("Writing data to target")
    writer.write(dataFrameWithColumns)
    logger.info("Launcher execution completed successfully")
  }

  private def buildReader(config: LauncherConfig)(implicit spark: SparkSession): SparkReader = config.sourceFormat match {
    case "jdbc" => 
      logger.error("JDBC source format is not supported")
      throw com.javi.personal.wallascala.WallaScalaException("JDBC source format is not supported yet.")
    case _ => new SparkFileReader(
      path = config.sourcePath.getOrElse {
        logger.error("Source path is required but not provided")
        throw com.javi.personal.wallascala.WallaScalaException("Source path is required.")
      }, 
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
