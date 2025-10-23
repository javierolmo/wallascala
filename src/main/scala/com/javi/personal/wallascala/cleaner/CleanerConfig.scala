package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.WallaScalaException
import com.javi.personal.wallascala.cleaner.model.MetadataCatalog
import scopt.{OParser, OParserBuilder}

case class CleanerConfig(sourcePath: String, targetPath: String, targetPathExclusions: String, id: String) {
  
  /**
   * Validates the configuration. Should be called after construction.
   * @throws WallaScalaException if validation fails
   */
  def validate(): Unit = {
    import com.javi.personal.wallascala.ValidationHelper._
    requireNonEmpty(sourcePath, "sourcePath")
    requireNonEmpty(targetPath, "targetPath")
    requireNonEmpty(targetPathExclusions, "targetPathExclusions")
    requireNonEmpty(id, "id")
  }
}

object CleanerConfig {

  private val PROGRAM_NAME = "wallascala"
  private val VERSION = "0.1"

  val builder: OParserBuilder[CleanerConfig] = OParser.builder[CleanerConfig]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      opt[String]('s', "sourcePath")
        .required()
        .action((x, c) => c.copy(sourcePath = x))
        .text("source path of the dataset to clean"),
      opt[String]('t', "targetPath")
        .required()
        .action((x, c) => c.copy(targetPath = x))
        .text("target path for the cleaned dataset"),
      opt[String]('e', "targetPathExclusions")
        .required()
        .action((x, c) => c.copy(targetPathExclusions = x))
        .text("target path for the cleaned dataset exclusions"),
      opt[String]('i', "id")
        .required()
        .action((x, c) => c.copy(id = x))
        .text(s"id of the cleaner metadata to use [${MetadataCatalog.default().availableIds().mkString(", ")}]"),
      help("help").text("prints this usage text")
    )
  }

  def parse(args: Array[String]): CleanerConfig = {
    val config = OParser.parse(parser, args, dummy)
      .getOrElse(throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]"))
    config.validate()
    config
  }

  def dummy: CleanerConfig = CleanerConfig(null, null, null, null)

}
