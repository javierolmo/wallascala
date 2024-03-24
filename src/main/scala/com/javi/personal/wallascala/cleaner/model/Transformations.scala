package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, regexp_replace}

object Transformations {

  def fromMillis(inputColumn: Column): Column = from_unixtime(inputColumn / 1000)

  def removeNonNumeric(inputColumn: Column): Column = regexp_replace(inputColumn, "[^0-9]", "")

  def removeLineBreaks(inputColumn: Column): Column = regexp_replace(inputColumn, "\n", "")

}
