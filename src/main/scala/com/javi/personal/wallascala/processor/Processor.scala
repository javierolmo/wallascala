package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.reflections.Reflections

import java.time.LocalDate
import scala.collection.JavaConverters._

abstract class Processor(config: ProcessorConfig, val dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) {

  protected def readSanited(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName)

  protected def readSanited(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readSanited(source, datasetName, date)

  protected def readSanitedOptional(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readSanitedOptional(source, datasetName, date)

  protected def readProcessed(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): DataFrame =
    dataSourceProvider.readProcessed(dataset, dateOption)

  protected def readProcessedOptional(dataset: ProcessedTables, dateOption: Option[LocalDate] = None)(implicit spark: SparkSession): Option[DataFrame] =
    dataSourceProvider.readProcessedOptional(dataset, dateOption)

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
    val cols = schema.fields.map(field => col(field.name).cast(field.dataType))
    val dataFrame = build().select(cols:_*)
    writer.write(dataFrame)(spark)
  }

}

object Processor {

  def build(tableName: String, date: LocalDate, targetPath: String)(implicit spark: SparkSession): Processor = {
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
    val elts: Seq[Class[_]] = new Reflections("com.javi.personal.wallascala.processor.etls")
      .getTypesAnnotatedWith(classOf[ETL]).asScala.toSeq
    val selectedEtl = elts
      .find(_.getAnnotation(classOf[ETL]).table().getName == config.datasetName)
      .getOrElse(throw new Exception(s"ETL not found for table ${config.datasetName}"))
    selectedEtl.getConstructors.head.newInstance(config, dataSourceProvider, spark).asInstanceOf[Processor]
  }

}
