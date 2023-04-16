package com.javi.personal
package wallascala.services.impl.blob.model

import org.apache.spark.sql.types.StructType

case class ReadConfig(
                       format: String = "delta",
                       schema: Option[StructType] = None,
                       options: Map[String, String] = Map()
                     )
