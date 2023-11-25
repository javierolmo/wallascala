package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.WallaScalaException
import scopt.{OParser, OParserBuilder}

case class LauncherConfig(
                           sourcePath: Option[String] = Option.empty,
                           sourceTable: Option[String] = Option.empty,
                           targetTable: Option[String] = Option.empty,
                           targetPath: Option[String] = Option.empty,
                           sourceFormat: String = "parquet",
                           targetFormat: String = "parquet",
                           coalesce: Option[Int] = Option.empty,
                           select: Option[Seq[String]] = Option.empty
                         )

object LauncherConfig {
  private val PROGRAM_NAME = "wallascala-ingestor"
  private val VERSION = "0.1"

  private val builder: OParserBuilder[LauncherConfig] = OParser.builder[LauncherConfig]

  private val parser = {
    import builder._
    OParser.sequence(
      programName(PROGRAM_NAME),
      head(PROGRAM_NAME, VERSION),
      opt[String]("sourcePath")
        .optional()
        .action((x, c) => c.copy(sourcePath = Some(x)))
        .text("source path"),
      opt[String]("sourceTable")
        .optional()
        .action((x, c) => c.copy(sourceTable = Some(x)))
        .text("source sql table"),
      opt[String]("targetPath")
        .optional()
        .action((x, c) => c.copy(targetPath = Some(x)))
        .text("target path"),
      opt[String]("targetTable")
        .optional()
        .action((x, c) => c.copy(targetTable = Some(x)))
        .text("target sql table"),
      opt[String]("sourceFormat")
        .optional()
        .action((x, c) => c.copy(sourceFormat = x))
        .text("source format"),
      opt[String]("targetFormat")
        .optional()
        .action((x, c) => c.copy(targetFormat = x))
        .text("source format"),
      opt[Seq[String]]("select")
        .optional()
        .action((x, c) => c.copy(select = Some(x)))
        .text("list of fields to partition by final source"),
      opt[Int]("coalesce")
        .optional()
        .action((x, c) => c.copy(coalesce = Some(x)))
        .text("Perform spark's coalesce in write operation"),
      help("help").text("prints this usage text"),
      checkConfig { config =>
        if (config.sourceFormat == "jdbc" && config.sourceTable.isEmpty)
          failure("sourceFormat = jdbc needs sourceTable parameter value")
        else if (config.targetFormat == "jdbc" && config.targetTable.isEmpty)
          failure("targetFormat = jdbc needs targetTable parameter value")
        else
          success
      }
    )
  }

  def parse(args: Array[String]): LauncherConfig = {
    OParser.parse(parser, args, LauncherConfig()) match {
      case Some(config) => config
      case None => throw WallaScalaException(f"Could not parse arguments: [${args.mkString(", ")}]")
    }
  }
}
