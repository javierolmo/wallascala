package com.javi.personal
package wallascala.services

import wallascala.services.impl.blob.model.{ReadConfig, StorageAccountLocation, WriteConfig}
import wallascala.services.impl.blob.{BlobServiceImpl, SparkSessionFactory}

import org.apache.spark.sql.DataFrame

object BlobService {

  val spark = SparkSessionFactory.build()

  def apply(secretService: SecretService) = {
    new BlobServiceImpl(spark, secretService)
  }

}

trait BlobService {

  def read(location: StorageAccountLocation, config: ReadConfig): DataFrame

  def readOptional(location: StorageAccountLocation, config: ReadConfig): Option[DataFrame]

  def write(dataFrame: DataFrame, location: StorageAccountLocation, config: WriteConfig): Unit

}
