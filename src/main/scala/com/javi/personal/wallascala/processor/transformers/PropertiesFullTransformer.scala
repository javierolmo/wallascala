package com.javi.personal.wallascala.processor.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object PropertiesFullTransformer {

  def deduplicateLatest(df: DataFrame): DataFrame = {
    df.withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy(col("modification_date").desc)))
      .filter(col("row_number") === 1)
      .drop("row_number")
  }

  def transform(wallapopProperties: DataFrame, pisosProperties: DataFrame): DataFrame = {
    val cleanWallapop = deduplicateLatest(wallapopProperties)
    val cleanPisos = deduplicateLatest(pisosProperties)
    cleanWallapop.unionByName(cleanPisos, allowMissingColumns = true)
  }

}
