package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.WallaScalaException
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class CleanerConfig(source: String, datasetName: String, date: Option[LocalDate] = Option.empty)

object CleanerConfig {

  private val PROGRAM_NAME = "wallascala"
  private val VERSION = "0.1"

  val builder: OParserBuilder[CleanerConfig] = OParser.builder[CleanerConfig]

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
        .optional()
        .action((x, c) => c.copy(date = Some(LocalDate.parse(x))))
        .text("date to clean"),
      help("help").text("prints this usage text")
    )
  }

  def parse(args: Array[String]): CleanerConfig =
    OParser.parse(parser, args, CleanerConfig(null, null)) match {
      case Some(config) => config
      case None => throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]")
    }

}
