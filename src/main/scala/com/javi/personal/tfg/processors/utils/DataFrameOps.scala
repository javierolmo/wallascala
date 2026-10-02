package com.javi.personal.tfg.processors.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

object DataFrameOps {
  implicit class DataFrameExtensionOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame

    def alignWithSchema(targetSchema: StructType): DataFrame = {
      val existingColumns = dataFrame.columns.toSet
      val cols = targetSchema.fields.map { field =>
        if (existingColumns.contains(field.name)) {
          col(field.name).cast(field.dataType).as(field.name)
        } else {
          lit(null).cast(field.dataType).as(field.name)
        }
      }
      dataFrame.select(cols: _*)
    }
  }
}
