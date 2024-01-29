package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.WallaScalaException
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class ProcessorConfig(datasetName: String, date: LocalDate) {
  def this(processedTable: ProcessedTables, date: LocalDate) = this(processedTable.toString, date)
}

object ProcessorConfig {

  private val PROGRAM_NAME = "processor"
  private val VERSION = "0.1"

  val builder: OParserBuilder[ProcessorConfig] = OParser.builder[ProcessorConfig]

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

  def parse(args: Array[String]): ProcessorConfig =
    OParser.parse(parser, args, ProcessorConfig(null, null)) match {
      case Some(config) => config
      case None => throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]")
    }

}
