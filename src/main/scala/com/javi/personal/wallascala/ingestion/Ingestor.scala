package com.javi.personal.wallascala.ingestion

import com.javi.personal.wallascala.PathBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

class Ingestor(spark: SparkSession) {

  def ingest(source: String, datasetName: String, fileName: String = ""): Unit = {

    val stagingJSON = spark.read
      .format("parquet")
      .option("header", "true")
      .load(s"${PathBuilder.buildStagingPath(source, datasetName).url}/year=*/month=*/day=*/*.parquet")

    val transformedDF = stagingJSON
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .withColumn("year", lpad(year(col("date")), 4, "0"))
      .withColumn("month", lpad(month(col("date")), 2, "0"))
      .withColumn("day", lpad(dayofmonth(col("date")), 2, "0"))

    transformedDF.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("year", "month", "day")
      .option("path", PathBuilder.buildRawPath(source, datasetName).url)
      .saveAsTable(s"raw.${source}_$datasetName")

  }

}