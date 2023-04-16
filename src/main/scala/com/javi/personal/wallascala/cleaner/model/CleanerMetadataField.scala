package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.types.DataType

case class CleanerMetadataField(name: String, fieldType: DataType)