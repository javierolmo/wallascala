package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.processor.tables.{PostalCodeAnalysis, PriceChanges, Properties}
import org.apache.spark.sql.SparkSession
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class ProcessorParams(datasetName: String, date: LocalDate)

object ProcessorCLI {

  private val PROGRAM_NAME = "processor"
  private val VERSION = "0.1"

  val builder: OParserBuilder[ProcessorParams] = OParser.builder[ProcessorParams]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      opt[String]('n', "datasetName")
        .required()
        .action((x, c) => c.copy(datasetName = x))
        .text("dataset to ingest"),
      opt[String]('d', "date")
        .required()
        .action((x, c) => c.copy(date = LocalDate.parse(x)))
        .text("date to clean"),
      help("help").text("prints this usage text")
    )
  }

  def main(args: Array[String]): Unit = {
    implicit lazy val spark: SparkSession = SparkSessionFactory.build()
    OParser.parse(parser, args, ProcessorParams("datasetName", LocalDate.of(1900, 1, 1))) match {
      case Some(params) =>
        val processor = params.datasetName match {
          case "properties" => Properties(Some(params.date))
          case "price_changes" => PriceChanges(Some(params.date))
          case "postal_code_analysis" => PostalCodeAnalysis(Some(params.date))
        }
        processor.execute()
      case _ =>
        throw new IllegalArgumentException("Invalid arguments: " + args.mkString(", ") + ".")
    }
  }

}
