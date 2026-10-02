package com.javi.personal.tfg.processors.cleaner.model

import org.apache.spark.sql.DataFrame

case class ValidationResult(validRecords: DataFrame, invalidRecords: DataFrame)
