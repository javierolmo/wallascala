package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace

object Transformations {

  def removeNonNumeric(inputColumn: Column): Column = regexp_replace(inputColumn, "[^0-9]", "")

  def removeLineBreaks(inputColumn: Column): Column = regexp_replace(inputColumn, "\n", "")

}
