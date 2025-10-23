package com.javi.personal.wallascala.utils

import org.apache.spark.sql.DataFrame

object DataFrameOps {
  implicit class DataFrameOps(dataFrame: DataFrame) {
    def applyIf(condition: Boolean, function: DataFrame => DataFrame): DataFrame =
      if (condition) function(dataFrame) else dataFrame
  }
}
