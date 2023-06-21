package com.javi.personal.wallascala.ingestion

import com.javi.personal.wallascala.SparkSessionFactory
import scopt.{OParser, OParserBuilder}

import java.time.LocalDate

case class IngestorParams(source: String, datasetName: String, date: LocalDate)

object IngestorCLI {

  private val PROGRAM_NAME = "wallascala-ingestor"
  private val VERSION = "0.1"

  val builder: OParserBuilder[IngestorParams] = OParser.builder[IngestorParams]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      opt[String]('s', "source")
        .required()
        .action((x, c) => c.copy(source = x))
        .text("source to ingest"),
      opt[String]('n', "datasetName")
        .required()
        .action((x, c) => c.copy(datasetName = x))
        .text("dataset to ingest"),
      help("help").text("prints this usage text")
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, IngestorParams("", "", LocalDate.of(1900, 1, 1))) match {
      case Some(params) =>
        val spark = SparkSessionFactory.build()
        val ingestor: Ingestor = new Ingestor(spark)
        ingestor.ingest(params.source, params.datasetName)
      case _ =>
        throw new IllegalArgumentException("Invalid arguments: " + args.mkString(", ") + ".")
    }
  }

}
