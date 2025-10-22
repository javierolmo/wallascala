package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.SparkUtils
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.reflections.Reflections

import java.time.LocalDate
import scala.collection.JavaConverters._

abstract class Processor(config: ProcessorConfig)(implicit spark: SparkSession) extends SparkUtils {

  protected val datasetName: ProcessedTables = getClass.getAnnotation(classOf[ETL]).table()
  protected val schema: StructType = StructType(Seq())
  protected def writer: SparkWriter = SparkFileWriter(
    path = config.targetPath,
    repartition = config.repartition
  )
  protected def build(): DataFrame

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
    val elts: Seq[Class[_]] = new Reflections("com.javi.personal.wallascala.processor.etls")
      .getTypesAnnotatedWith(classOf[ETL]).asScala.toSeq
    val selectedEtl = elts
      .find(_.getAnnotation(classOf[ETL]).table().getName == config.datasetName)
      .getOrElse(throw new Exception(s"ETL not found for table ${config.datasetName}"))
    selectedEtl.getConstructors.head.newInstance(config, spark).asInstanceOf[Processor]
  }

}
