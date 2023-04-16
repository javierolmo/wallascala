package com.javi.personal
package wallascala.services.impl.blob.model

import org.apache.spark.sql.SaveMode

case class WriteConfig(
                        format: String = "delta",
                        saveMode: SaveMode = SaveMode.ErrorIfExists,
                        options: Map[String, String] = Map(),
                        partitionColumns: Seq[String] = Seq()
                      )
