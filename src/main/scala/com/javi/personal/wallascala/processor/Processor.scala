package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.tables._
import com.javi.personal.wallascala.utils.writers.{SparkFileWriter, SparkWriter}
import com.javi.personal.wallascala.{PathBuilder, SparkUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate

abstract class Processor(date: LocalDate)(implicit spark: SparkSession) extends SparkUtils {

  protected val datasetName: ProcessedTables
  protected val coalesce: Option[Int] = Option.empty
  protected val finalColumns: Array[String] = Array.empty
  protected val schema: StructType = StructType(Seq())
  protected def writers: Seq[SparkWriter] = Seq(
    SparkFileWriter(PathBuilder.buildProcessedPath(datasetName.getName).cd(date).url)(spark)
  )
  protected def build(): DataFrame

  final def execute(): Unit = {
    val cols: Array[Column] = schema.fields.map(field => col(field.name).cast(field.dataType))
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
      case "fotocasa_properties" => FotocasaProperties(config.date)
      case "wallapop_properties" => new WallapopProperties(config.date)
      case "properties" => new Properties(config.date)
      case "price_changes" => PriceChanges(config.date)
      case "postal_code_analysis" => PostalCodeAnalysis(config.date)
    }
  }

}
