package com.javi.personal.wallascala.controller

import com.javi.personal.wallascala.model.catalog.CatalogItem
import com.javi.personal.wallascala.model.cleaner.Cleaner
import com.javi.personal.wallascala.model.egestor.Egestor
import com.javi.personal.wallascala.model.ingestion.Ingestor
import com.javi.personal.wallascala.model.processor.Processor
import com.javi.personal.wallascala.model.services.impl.blob.SparkSessionFactory
import org.apache.spark.sql.SparkSession

object MainController {

  def apply(spark: SparkSession = SparkSessionFactory.build()): MainController = {
    val ingestor = new Ingestor(spark)
    val cleaner = new Cleaner(null) //TODO:
    val processor = new Processor(spark)
    val egestor = new Egestor(spark)
    new MainController(ingestor, cleaner, processor, egestor)
  }

}

class MainController(ingestor: Ingestor, cleaner: Cleaner, processor: Processor, egestor: Egestor) {

  def ingest(catalogItem: CatalogItem): Unit = {
    ingestor.ingest(catalogItem)
  }

  def clean(): Unit = {

  }

  def process(): Unit = {

  }

  def egest(): Unit = {

  }

}
