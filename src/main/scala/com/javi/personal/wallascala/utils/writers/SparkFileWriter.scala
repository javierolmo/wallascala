package com.javi.personal.wallascala.utils.writers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkFileWriter is a class that defines the basic methods to write a DataFrame to a file.
 * @param path The path of the DataFrame to write.
 * @param hiveTable The hive table of the DataFrame to write.
 * @param format The format of the DataFrame to write.
 * @param saveMode The save mode of the DataFrame to write.
 * @param options The options of the DataFrame to write.
 * @param coalesce The number of partitions of the DataFrame to write.
 * @param spark The SparkSession to use.
 */
case class SparkFileWriter
  (path: String, hiveTable: Option[String] = None, format: String = "parquet", saveMode: String = "overwrite", options: Map[String, String] = Map(), coalesce: Option[Int] = None, partitionBy: Seq[String] = Seq(), repartition: Option[Int] = None)
  (implicit spark: SparkSession)
extends SparkWriter(format=format, saveMode=saveMode, options=options, partitionBy=partitionBy) {

  private def withCoalesce(dataFrame: DataFrame): DataFrame = 
    coalesce.map(dataFrame.coalesce).getOrElse(dataFrame)

  private def withRepartition(dataFrame: DataFrame): DataFrame = 
    repartition.map(dataFrame.repartition).getOrElse(dataFrame)

  def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
    val writer = baseWriter(withRepartition(withCoalesce(dataFrame)))
    hiveTable.fold(writer.save(path))(table => writer.option("path", path).saveAsTable(table))
  }
}

object SparkFileWriter {

  def write(dataFrame: DataFrame, path: String, hiveTable: Option[String] = None, format: String = "parquet", saveMode: String = "overwrite", options: Map[String, String] = Map(), coalesce: Option[Int] = None, partitionBy: Seq[String] = Seq()) (implicit spark: SparkSession): Unit =
    SparkFileWriter(path, hiveTable, format, saveMode, options, coalesce, partitionBy).write(dataFrame)

}
