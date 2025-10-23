package com.javi.personal.wallascala

/**
 * Central configuration for database names used throughout the application.
 * This object provides a single source of truth for database naming,
 * improving maintainability and reducing the risk of typos.
 */
object DatabaseNames {
  val RAW = "raw"
  val SANITED = "sanited"
  val EXCLUDED = "excluded"
  val PROCESSED = "processed"

  /**
   * Returns all database names that should be initialized at startup.
   */
  def all: Seq[String] = Seq(RAW, SANITED, EXCLUDED, PROCESSED)
}
