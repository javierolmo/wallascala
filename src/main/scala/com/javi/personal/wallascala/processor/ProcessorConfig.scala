package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.WallaScalaException
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class ProcessorConfig(datasetName: String, date: LocalDate, targetPath: String, coalesce: Option[Int] = None, repartition: Option[Int] = None) {
  
  /**
   * Validates the configuration. Should be called after construction.
   * @throws WallaScalaException if validation fails
   */
  def validate(): Unit = {
    import com.javi.personal.wallascala.ValidationHelper._
    requireNonEmpty(datasetName, "datasetName")
    requireNonEmpty(targetPath, "targetPath")
    coalesce.foreach(c => requirePositive(c, "coalesce"))
    repartition.foreach(r => requirePositive(r, "repartition"))
  }
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
      opt[String]('r', "repartition")
        .optional()
        .action((x, c) => c.copy(repartition = Some(x.toInt)))
        .text("number of partitions to coalesce the data"),
      help("help").text("prints this usage text")
    )
  }

  def parse(args: Array[String]): ProcessorConfig = {
    val config = OParser.parse(parser, args, dummy)
      .getOrElse(throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]"))
    config.validate()
    config
  }

  private def dummy: ProcessorConfig = ProcessorConfig("", LocalDate.now, "")

}
