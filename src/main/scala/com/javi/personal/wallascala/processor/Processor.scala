package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.SparkUtils
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.reflections.Reflections

import java.time.LocalDate
import scala.collection.JavaConverters._

abstract class Processor(config: ProcessorConfig)(implicit spark: SparkSession) extends SparkUtils with com.javi.personal.wallascala.Logging {

  protected val datasetName: ProcessedTables = getClass.getAnnotation(classOf[ETL]).table()
  protected val schema: StructType = StructType(Seq())
  protected def writer: SparkWriter = SparkFileWriter(
    path = config.targetPath,
    repartition = config.repartition
  )
  protected def build(): DataFrame

  // Public method for testing
  def buildForTesting(): DataFrame = build()

  final def execute(): Unit = {
    logger.info("Starting processor execution for dataset: {} on date: {}", datasetName.getName, config.date)
    val cols = schema.fields.map(field => col(field.name).cast(field.dataType))
    logger.debug("Building dataframe for dataset: {}", datasetName.getName)
    val dataFrame = build().select(cols:_*)
    logger.info("Dataframe built successfully, row count: {}", dataFrame.count())
    logger.info("Writing data to: {}", config.targetPath)
    writer.write(dataFrame)(spark)
    logger.info("Processor execution completed successfully for dataset: {}", datasetName.getName)
  }

}

object Processor extends com.javi.personal.wallascala.Logging {

  def build(tableName: String, date: LocalDate, targetPath: String)(implicit spark: SparkSession): Processor = {
    logger.debug("Building processor for table: {}", tableName)
    val config = ProcessorConfig(tableName, date, targetPath)
    build(config)
  }

  def build(table: ProcessedTables, date: LocalDate, targetPath: String)(implicit spark: SparkSession): Processor = {
    val config = ProcessorConfig(table.getName, date, targetPath)
    build(config)
  }

  def build(config: ProcessorConfig)(implicit spark: SparkSession): Processor = {
    build(config, new DefaultDataSourceProvider())
  }

  def build(config: ProcessorConfig, dataSourceProvider: DataSourceProvider)(implicit spark: SparkSession): Processor = {
    logger.info("Building processor for dataset: {}", config.datasetName)
    val elts: Seq[Class[_]] = new Reflections("com.javi.personal.wallascala.processor.etls")
      .getTypesAnnotatedWith(classOf[ETL]).asScala.toSeq
    logger.debug("Found {} ETL classes", elts.size)
    val selectedEtl = elts
      .find(_.getAnnotation(classOf[ETL]).table().getName == config.datasetName)
      .getOrElse {
        logger.error("ETL not found for table: {}", config.datasetName)
        throw com.javi.personal.wallascala.WallaScalaException(s"ETL not found for table ${config.datasetName}")
      }
    logger.debug("Selected ETL class: {}", selectedEtl.getName)
    selectedEtl.getConstructors.head.newInstance(config, dataSourceProvider, spark).asInstanceOf[Processor]
  }

}
