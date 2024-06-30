package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.etls._
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import com.javi.personal.wallascala.{PathBuilder, SparkUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.reflections.Reflections

import java.time.LocalDate
import scala.collection.JavaConverters._

abstract class Processor(date: LocalDate)(implicit spark: SparkSession) extends SparkUtils {

  protected val datasetName: ProcessedTables = getClass.getAnnotation(classOf[ETL]).table()
  protected val writerCoalesce: Option[Int] = Option.empty
  protected val schema: StructType = StructType(Seq())
  protected val partitionByDate: Option[LocalDate] = Some(date)
  protected def writers: Seq[SparkWriter] = Seq(
    SparkFileWriter(
      path = PathBuilder.buildProcessedPath(datasetName.getName).url,
      hiveTable = Some(s"processed.$datasetName"),
      partitionBy = if (partitionByDate.isDefined) Seq("year", "month", "day") else Seq()
    )
  )
  protected def build(): DataFrame

  final def execute(): Unit = {
    val cols: Array[Column] = schema.fields.map(field => col(field.name).cast(field.dataType))
    val dataFrame = build().select(cols:_*)
    val dataFrameWithYearMonthDay = partitionByDate match {
      case Some(date) => dataFrame
        .withColumn("year", lit("%04d".format(date.getYear)))
        .withColumn("month", lit("%02d".format(date.getMonthValue)))
        .withColumn("day", lit("%02d".format(date.getDayOfMonth)))
      case None => dataFrame
    }
    val dataFrameWithCoalesce = if (writerCoalesce.isDefined) dataFrameWithYearMonthDay.coalesce(writerCoalesce.get) else dataFrameWithYearMonthDay

    // Write dataframe
    val cachedDF = if (writers.size > 1) dataFrameWithCoalesce.cache() else dataFrameWithCoalesce
    writers.foreach(writer => writer.write(cachedDF)(spark))
  }

}

object Processor {

  def build(tableName: String, date: LocalDate)(implicit spark: SparkSession): Processor = {
    val config = ProcessorConfig(tableName, date)
    build(config)
  }

  def build(table: ProcessedTables, date: LocalDate)(implicit spark: SparkSession): Processor = {
    val config = ProcessorConfig(table.getName, date)
    build(config)
  }

  def build(config: ProcessorConfig)(implicit spark: SparkSession): Processor = {
    val elts: Seq[Class[_]] = new Reflections("com.javi.personal.wallascala.processor.etls")
      .getTypesAnnotatedWith(classOf[ETL]).asScala.toSeq
    val selectedEtl = elts
      .find(_.getAnnotation(classOf[ETL]).table().getName == config.datasetName)
      .getOrElse(throw new Exception(s"ETL not found for table ${config.datasetName}"))
    selectedEtl.getConstructors.head.newInstance(config.date, spark).asInstanceOf[Processor]
  }

}
