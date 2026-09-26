package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.etls.{PisosProperties, PropertiesFullProcessor, WallapopProperties}
import com.javi.personal.wallascala.utils.DataFrameOps._
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.reflections.Reflections

import java.time.LocalDate
import scala.jdk.CollectionConverters._

abstract class Processor(
  config: ProcessorConfig,
  val dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider(),
  val customWriter: Option[SparkWriter] = None
)(implicit spark: SparkSession) {

  protected val datasetName: ProcessedTables = getClass.getAnnotation(classOf[ETL]).table()
  protected val schema: StructType
  protected def writer: SparkWriter = customWriter.getOrElse(
    SparkFileWriter(
      path = config.targetPath,
      repartition = config.repartition,
      coalesce = config.coalesce
    )
  )
  protected def build(): DataFrame

  final def execute(): DataFrame = {
    val dataFrame = build().alignWithSchema(schema)
    writer.write(dataFrame)(spark)
    dataFrame
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
    build(config, dataSourceProvider, None)
  }

  def build(config: ProcessorConfig, dataSourceProvider: DataSourceProvider, customWriter: Option[SparkWriter])(implicit spark: SparkSession): Processor = {
    config.datasetName match {
      case "wallapop_properties" => new WallapopProperties(config, dataSourceProvider, customWriter)
      case "pisos_properties" => new PisosProperties(config, dataSourceProvider, customWriter)
      case "properties_full" => new PropertiesFullProcessor(config, dataSourceProvider, customWriter)
      case _ =>
        val elts: Seq[Class[_]] = new Reflections("com.javi.personal.wallascala.processor.etls")
          .getTypesAnnotatedWith(classOf[ETL]).asScala.toSeq
        val selectedEtl = elts
          .find(_.getAnnotation(classOf[ETL]).table().getName == config.datasetName)
          .getOrElse(throw new Exception(s"ETL not found for table ${config.datasetName}"))
        try {
          selectedEtl.getConstructor(classOf[ProcessorConfig], classOf[DataSourceProvider], classOf[Option[SparkWriter]], classOf[SparkSession])
            .newInstance(config, dataSourceProvider, customWriter, spark).asInstanceOf[Processor]
        } catch {
          case _: NoSuchMethodException =>
            selectedEtl.getConstructors.head.newInstance(config, dataSourceProvider, spark).asInstanceOf[Processor]
        }
    }
  }

}
