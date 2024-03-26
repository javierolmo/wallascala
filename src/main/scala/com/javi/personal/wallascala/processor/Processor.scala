package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.tables._
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import com.javi.personal.wallascala.{PathBuilder, SparkUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

abstract class Processor(date: LocalDate)(implicit spark: SparkSession) extends SparkUtils {

  protected val datasetName: ProcessedTables
  protected val coalesce: Option[Int] = Option.empty
  protected val schema: StructType = StructType(Seq())
  protected def writers: Seq[SparkWriter] = Seq(
    SparkFileWriter(
      path = PathBuilder.buildProcessedPath(datasetName.getName).url,
      hiveTable = Some(s"processed.$datasetName"),
      partitionBy = Seq("year", "month", "day")
    )
  )
  protected def build(): DataFrame

  final def execute(): Unit = {
    val cols: Array[Column] = schema.fields.map(field => col(field.name).cast(field.dataType))
    val dataFrame = build().select(cols:_*)
    val dataFrameWithYearMonthDay = dataFrame
      .withColumn("year", lit("%04d".format(date.getYear)))
      .withColumn("month", lit("%02d".format(date.getMonthValue)))
      .withColumn("day", lit("%02d".format(date.getDayOfMonth)))
    val dataFrameWithCoalesce = if (coalesce.isDefined) dataFrameWithYearMonthDay.coalesce(coalesce.get) else dataFrameWithYearMonthDay

    // Write dataframe
    val cachedDF = if (writers.size > 1) dataFrameWithCoalesce.cache() else dataFrameWithCoalesce
    writers.foreach(writer => writer.write(cachedDF)(spark))
  }

}

object Processor {

  def build(config: ProcessorConfig)(implicit spark: SparkSession): Processor = {
    config.datasetName match {
      case "properties" => new Properties(config.date)
      case "price_changes" => PriceChanges(config.date)
      case "postal_code_analysis" => PostalCodeAnalysis(config.date)
    }
  }

}
