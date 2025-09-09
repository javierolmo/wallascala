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
                           select: Option[Seq[String]] = Option.empty,
                           flattenFields: Boolean = false,
                           newColumns: Seq[String] = Seq.empty,
                           mode: Option[String] = Option.empty
                         )

object LauncherConfig {
  private val PROGRAM_NAME = "launcher"
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
        .text("Ruta de origen del fichero o directorio a leer (por ejemplo, abfss://container@account/some_path). Solo se usa si sourceFormat no es 'jdbc'."),
      opt[String]("sourceTable")
        .optional()
        .action((x, c) => c.copy(sourceTable = Some(x)))
        .text("Nombre de la tabla SQL de origen (por ejemplo, db.tabla). Solo se usa si sourceFormat es 'jdbc'."),
      opt[String]("targetPath")
        .optional()
        .action((x, c) => c.copy(targetPath = Some(x)))
        .text("Ruta de destino donde se guardarán los datos (por ejemplo, abfss://container@account/some_path). Solo se usa si targetFormat no es 'jdbc'."),
      opt[String]("targetTable")
        .optional()
        .action((x, c) => c.copy(targetTable = Some(x)))
        .text("Nombre de la tabla SQL de destino (por ejemplo, db.tabla). Solo se usa si targetFormat es 'jdbc'."),
      opt[String]("sourceFormat")
        .optional()
        .action((x, c) => c.copy(sourceFormat = x))
        .text("Formato de los datos de entrada: parquet, csv, json, jdbc, etc. Por defecto: parquet."),
      opt[String]("targetFormat")
        .optional()
        .action((x, c) => c.copy(targetFormat = x))
        .text("Formato de los datos de salida: parquet, csv, json, jdbc, etc. Por defecto: parquet."),
      opt[Seq[String]]("select")
        .optional()
        .action((x, c) => c.copy(select = Some(x)))
        .text("Lista de campos a seleccionar del origen, separados por coma (por ejemplo, campo1,campo2)."),
      opt[Int]("coalesce")
        .optional()
        .action((x, c) => c.copy(coalesce = Some(x)))
        .text("Número de particiones a usar en la escritura (Spark coalesce)."),
      opt[Unit]("flattenFields")
        .optional()
        .action((_, c) => c.copy(flattenFields = true))
        .text("Si se indica, aplanará los campos anidados del origen (flatten)."),
      opt[Seq[String]]("addColumns")
        .optional()
        .action((x, c) => c.copy(newColumns = x))
        .text("Añade columnas al DataFrame final. Formato: nombre=valor. Ejemplo: col1=valor1,col2=valor2."),
      opt[String]("mode")
        .optional()
        .action((x, c) => c.copy(mode = Some(x)))
        .text("Modo de escritura: overwrite, append, etc. Por defecto: overwrite."),
      help("help").text("Muestra este texto de ayuda y termina."),
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
