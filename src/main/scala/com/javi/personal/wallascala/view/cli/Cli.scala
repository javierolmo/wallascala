package com.javi.personal.wallascala.view.cli

import com.javi.personal.wallascala.controller.MainController
import com.javi.personal.wallascala.view.cli.model.{CLEAN, Config, EXTRACT}
import scopt.{OParser, OParserBuilder}

object Cli {

  private val PROGRAM_NAME = "wallascala"
  private val VERSION = "0.1"

  private val cleanController: MainController = MainController()

  val builder: OParserBuilder[Config] = OParser.builder[Config]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      cmd("EXTRACT")
        .action((_, c) => c.copy(mode = Some(EXTRACT)))
        .text("Extract data from source"),
      cmd("CLEAN")
        .action((_, c) => c.copy(mode = Some(CLEAN)))
        .text("Clean data"),
      help("help").text("prints this usage text")
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser = parser, args = args, init = Config()) match {
      case Some(config) => config.mode match {
        case Some(CLEAN) => cleanController.clean()
        case None => println("No mode selected")
      }
      case _ => println("No mode selected")
    }
  }

}
