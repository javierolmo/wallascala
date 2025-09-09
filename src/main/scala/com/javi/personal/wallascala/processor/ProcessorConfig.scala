package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.WallaScalaException
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class ProcessorConfig(datasetName: String, date: LocalDate, targetPath: String, coalesce: Option[Int] = Option.empty, repartition: Option[Int] = Option.empty)

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
        .action((x, c) => {
          val paddedDate = x.split("-")
            .map(part => if (part.length == 1) f"0$part" else part)
            .mkString("-")
          c.copy(date = LocalDate.parse(paddedDate))
        })
        .text("date to process in format yyyy-MM-dd"),
      opt[String]('t', "targetPath")
        .required()
        .action((x, c) => c.copy(targetPath = x))
        .text("target path to write the processed data"),
      opt[String]('c', "coalesce")
        .optional()
        .validate(x =>
          if (x.toInt > 0) success
          else failure("Value for --coalesce must be a positive integer")
        )
        .action((x, c) => c.copy(coalesce = Some(x.toInt)))
        .text("number of partitions to coalesce the data"),
      opt[String]('r', "repartition")
        .optional()
        .validate(x =>
          if (x.toInt > 0) success
          else failure("Value for --repartition must be a positive integer")
        )
        .action((x, c) => c.copy(repartition = Some(x.toInt)))
        .text("number of partitions to repartition the data"),
      help("help").text("prints this usage text")
    )
  }

  def parse(args: Array[String]): ProcessorConfig =
    OParser.parse(parser, args, dummy) match {
      case Some(config) => config
      case None => throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]")
    }

  private def dummy: ProcessorConfig = ProcessorConfig("", LocalDate.now, "")

}
