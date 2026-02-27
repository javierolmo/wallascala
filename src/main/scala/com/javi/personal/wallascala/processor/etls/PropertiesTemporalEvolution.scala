package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PROPERTIES_TEMPORAL_EVOLUTION)
class PropertiesTemporalEvolution(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

  override protected val schema: StructType = StructType(Array(
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("province", StringType),
    StructField("operation", StringType),
    StructField("property_count", LongType),
    StructField("avg_price", DoubleType),
    StructField("avg_surface", DoubleType)
  ))

  private object sources {
    lazy val propertiesFull: DataFrame = dataSourceProvider.readGold(ProcessedTables.PROPERTIES_FULL)
  }

  override protected def build(): DataFrame =
    sources.propertiesFull
      .filter(col("modification_date").isNotNull)
      .withColumn("year", year(col("modification_date")))
      .withColumn("month", month(col("modification_date")))
      .groupBy("year", "month", "province", "operation")
      .agg(
        count("id").as("property_count"),
        round(avg("price"), 2).as("avg_price"),
        round(avg("surface"), 2).as("avg_surface")
      )
      .orderBy("year", "month", "province", "operation")

}
