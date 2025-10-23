package com.javi.personal.wallascala

import org.slf4j.{Logger, LoggerFactory}

/**
 * Trait to provide logging capabilities to classes.
 * Mix this trait into any class that needs logging functionality.
 * 
 * Usage:
 * {{{
 * class MyClass extends Logging {
 *   logger.info("This is a log message")
 * }
 * }}}
 */
trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
