package com.javi.personal.wallascala

/**
 * Constants for data format types used throughout the application.
 * Centralizing these values helps prevent typos and makes refactoring easier.
 */
object DataFormat {
  val PARQUET = "parquet"
  val JSON = "json"
  val CSV = "csv"
  val JDBC = "jdbc"
  val DELTA = "delta"
  val ORC = "orc"
  val AVRO = "avro"

  /**
   * Supported file-based formats (non-JDBC)
   */
  val FILE_FORMATS: Set[String] = Set(PARQUET, JSON, CSV, DELTA, ORC, AVRO)

  /**
   * All supported formats including JDBC
   */
  val ALL_FORMATS: Set[String] = FILE_FORMATS + JDBC

  /**
   * Validates if a format is supported
   */
  def isSupported(format: String): Boolean = ALL_FORMATS.contains(format)

  /**
   * Validates if a format is file-based (not JDBC)
   */
  def isFileFormat(format: String): Boolean = FILE_FORMATS.contains(format)
}
