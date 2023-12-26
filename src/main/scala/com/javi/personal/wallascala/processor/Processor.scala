package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.tables.{PostalCodeAnalysis, PriceChanges, Properties}
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import com.javi.personal.wallascala.{PathBuilder, SparkUtils}
import org.apache.spark.sql.functions.{col, lit, lpad}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

abstract class Processor(date: LocalDate)(implicit spark: SparkSession) extends SparkUtils {

  protected val datasetName: ProcessedTables
  protected val finalColumns: Array[String]
  protected val coalesce: Option[Int] = Option.empty
  protected def writers: Seq[SparkWriter] = Seq(
    SparkFileWriter(PathBuilder.buildProcessedPath(datasetName.getName).cd(date).url)(spark)
  )
  protected def build(): DataFrame

  final def execute(): Unit = {
    val cols: Array[Column] = finalColumns.map(colName => col(colName))
    val dataFrame = build().select(cols:_*)
    val dataFrameWithCoalesce = if (coalesce.isDefined) dataFrame.coalesce(coalesce.get) else dataFrame

    // Write dataframe
    val cachedDF = dataFrameWithCoalesce.cache()
    writers.foreach(writer => writer.write(cachedDF)(spark))
  }

}

object Processor {

  def build(config: ProcessorConfig)(implicit spark: SparkSession): Processor = {
    config.datasetName match {
      case "properties" => Properties(config.date)
      case "price_changes" => PriceChanges(config.date)
      case "postal_code_analysis" => PostalCodeAnalysis(config.date)
    }
  }

}
