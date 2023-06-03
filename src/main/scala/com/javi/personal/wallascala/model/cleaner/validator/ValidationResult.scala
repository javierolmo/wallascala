package com.javi.personal.wallascala.model.cleaner.validator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class ValidationResult(validRecords: DataFrame, invalidRecords: DataFrame) {

  def withYearMonthDay(localDate: LocalDate): ValidationResult = {
    val yearString = DateTimeFormatter.ofPattern("yyyy").format(localDate)
    val monthString = DateTimeFormatter.ofPattern("MM").format(localDate)
    val dayString = DateTimeFormatter.ofPattern("dd").format(localDate)
    ValidationResult(
      validRecords = validRecords
        .withColumn("year", lit(yearString))
        .withColumn("month", lit(monthString))
        .withColumn("day", lit(dayString)),
      invalidRecords = invalidRecords
        .withColumn("year", lit(yearString))
        .withColumn("month", lit(monthString))
        .withColumn("day", lit(dayString))
    )
  }

}
