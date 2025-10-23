package com.javi.personal.wallascala

/**
 * Constants for save modes used when writing data.
 * These correspond to Spark's SaveMode options.
 */
object SaveMode {
  val OVERWRITE = "overwrite"
  val APPEND = "append"
  val ERROR_IF_EXISTS = "errorifexists"
  val IGNORE = "ignore"

  /**
   * All supported save modes
   */
  val ALL_MODES: Set[String] = Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)

  /**
   * Validates if a save mode is supported
   */
  def isSupported(mode: String): Boolean = ALL_MODES.contains(mode.toLowerCase)
}
