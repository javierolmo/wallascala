package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PROPERTIES_BY_TYPE)
class PropertiesByType(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

  override protected val schema: StructType = StructType(Array(
    StructField("type", StringType),
    StructField("operation", StringType),
    StructField("province", StringType),
    StructField("property_count", LongType),
    StructField("avg_price", DoubleType),
    StructField("avg_surface", DoubleType),
    StructField("avg_rooms", DoubleType),
    StructField("avg_bathrooms", DoubleType)
  ))

  private object sources {
    lazy val propertiesFull: DataFrame = dataSourceProvider.readGold(ProcessedTables.PROPERTIES_FULL)
  }

  override protected def build(): DataFrame =
    sources.propertiesFull
      .groupBy("type", "operation", "province")
      .agg(
        count("id").as("property_count"),
        round(avg("price"), 2).as("avg_price"),
        round(avg("surface"), 2).as("avg_surface"),
        round(avg("rooms"), 2).as("avg_rooms"),
        round(avg("bathrooms"), 2).as("avg_bathrooms")
      )

}
