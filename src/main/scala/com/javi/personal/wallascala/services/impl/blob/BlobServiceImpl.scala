package com.javi.personal
package wallascala.services.impl.blob

import com.javi.personal.wallascala.services.impl.blob.model.{ReadConfig, StorageAccountLocation, WriteConfig}
import com.javi.personal.wallascala.services.{BlobService, SecretService}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class BlobServiceImpl(spark: SparkSession, secretService: SecretService) extends BlobService {

  private val log = LogManager.getLogger(this.getClass)

  override def read(location: StorageAccountLocation, config: ReadConfig): DataFrame = { //TODO: Estudiar si config debería ser obligatorio. Ha ocurrido de intentar leer un CSV y el formato estar por defecto a delta
    checkAuthentication(location)
    read(location.url, config)
  }

  override def readOptional(location: StorageAccountLocation, config: ReadConfig): Option[DataFrame] = {
    try {
      val result = read(location, config)
      Some(result)
    } catch {
      case t: Throwable =>
        log.error("Error reading from " + location.url + ". " + t.getMessage)
        Option.empty
    }

  }

  override def write(dataFrame: DataFrame, location: StorageAccountLocation, config: WriteConfig): Unit = {
    checkAuthentication(location)
    write(dataFrame, location.url, config)
  }

  private def write(dataFrame: DataFrame, path: String, config: WriteConfig) = {
    log.debug(s"Writting data to path '$path' with config: $config")
    dataFrame.write
      .format(config.format)
      .partitionBy(config.partitionColumns: _*)
      .mode(config.saveMode)
      .options(config.options)
      .save(path)
  }

  private def read(url: String, config: ReadConfig): DataFrame = {
    log.debug(s"Reading data from '$url' with config: $config")
    val dataFrameReader = spark.read
      .format(config.format)
      .options(config.options)
    val dataFrameReaderWithSchema = config.schema match {
      case Some(schema) => dataFrameReader.schema(schema)
      case None => dataFrameReader
    }
    dataFrameReaderWithSchema.load(url)
  }

  private def checkAuthentication(location: StorageAccountLocation): Unit = {
    log.debug(s"Checking authentication for storage account: '${location.account}'")
    if (true) { //FIXME: Provisional hotfix. En databricks, después de la primera ejecución, la segunda detecta que ya hay claves y no las registra. A pesar de esto, luego al intentar leer falla.
      val key = secretService.getSecret(location.account + "-key")
      val version = if (location.v2) "dfs" else "blob"
      val configName = f"fs.azure.account.key.${location.account}.${version}.core.windows.net"
      spark.conf.set(configName, key)
      spark.sparkContext.hadoopConfiguration.set(configName, key)
      log.info(s"Added '${location.account}' secret to spark config: ${configName}")
    } else {
      log.debug(f"Skipped authentication to storage account '${location.account}'")
    }
  }

}
