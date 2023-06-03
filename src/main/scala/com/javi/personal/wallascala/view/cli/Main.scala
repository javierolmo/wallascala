package com.javi.personal.wallascala.view.cli

import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.services.impl.blob.model.ReadConfig
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}

object Main {

  def main(args: Array[String]): Unit = {
    val df = BlobService(SecretService()).read(DataCatalog.PISO_WALLAPOP.sanitedLocation, ReadConfig(format = "parquet"))
    df.write.format("mongodb").mode("overwrite").save()
  }

}
