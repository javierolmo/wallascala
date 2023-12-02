package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.DataFrame

case class ValidationResult(validRecords: DataFrame, invalidRecords: DataFrame)
