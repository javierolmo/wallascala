package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.utils.reader.{BlobStorageReader, Reader}
import com.javi.personal.wallascala.utils.writers.{SqlWriter, Writer}
import org.apache.spark.sql.SparkSession

object Launcher {

  private implicit lazy val spark: SparkSession = SparkSessionFactory.build()

  def run(reader: Reader, writer: Writer): Unit = {
    val dataFrame = reader.read()
    writer.write(dataFrame)
  }

  def main(args: Array[String]): Unit = {
    val layer = "processed"
    val dataset = "properties"
    val reader = BlobStorageReader(layer, dataset)
    val writer = SqlWriter(layer, dataset)
    Launcher.run(reader, writer)
  }

}
