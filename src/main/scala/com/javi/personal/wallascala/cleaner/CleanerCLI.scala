package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.SparkSessionFactory
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class CleanerParams(source: String, datasetName: String, date: LocalDate)

object CleanerCLI {

  private val PROGRAM_NAME = "wallascala"
  private val VERSION = "0.1"

  val builder: OParserBuilder[CleanerParams] = OParser.builder[CleanerParams]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      opt[String]('s', "source")
        .required()
        .action((x, c) => c.copy(source = x))
        .text("source to clean"),
      opt[String]('n', "datasetName")
        .required()
        .action((x, c) => c.copy(datasetName = x))
        .text("dataset name to clean"),
      opt[String]('d', "date")
        .required()
        .action((x, c) => c.copy(date = LocalDate.parse(x)))
        .text("date to clean"),
      help("help").text("prints this usage text")
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, CleanerParams("", "", LocalDate.of(1900, 1, 1))) match {
      case Some(params) =>
        val spark = SparkSessionFactory.build()
        val cleaner = new Cleaner(spark)
        cleaner.execute(params.source, params.datasetName, params.date)
      case _ =>
        throw new IllegalArgumentException("Invalid arguments: " + args.mkString(", ") + ".")
    }
  }

}
