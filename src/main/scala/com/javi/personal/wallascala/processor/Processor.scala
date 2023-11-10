package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.tables.{PostalCodeAnalysis, PriceChanges, Properties}
import com.javi.personal.wallascala.utils.writers.{ParquetWriter, Writer}
import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory, SparkUtils}
import org.apache.spark.sql.functions.{col, days}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.io.PrintWriter
import java.time.LocalDate

object Processor {

  private lazy implicit val spark: SparkSession = SparkSessionFactory.build()

  def properties(): Processor = Properties()
  def properties(date: LocalDate): Processor = Properties(Some(date))
  def priceChanges(): Processor = PriceChanges()
  def priceChanges(date: LocalDate): Processor = PriceChanges(Some(date))
  def postalCodeAnalysis(): Processor = PostalCodeAnalysis()
  def postalCodeAnalysis(date: LocalDate): Processor = PostalCodeAnalysis(date)

}


abstract class Processor(spark: SparkSession) extends SparkUtils {

  protected val datasetName: String
  protected val finalColumns: Array[String]
  protected val coalesce: Option[Int] = Option.empty
  protected def writers: Seq[Writer] = Seq(ParquetWriter("processed", datasetName))
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
