package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

@ETL(table = ProcessedTables.PROPERTIES_SPECIAL_FEATURES)
class PropertiesSpecialFeatures(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

  override protected val schema: StructType = StructType(Array(
    StructField("province", StringType),
    StructField("operation", StringType),
    StructField("property_count", LongType),
    StructField("pool_ratio", DoubleType),
    StructField("garage_ratio", DoubleType),
    StructField("garden_ratio", DoubleType),
    StructField("terrace_ratio", DoubleType),
    StructField("elevator_ratio", DoubleType)
  ))

  private object sources {
    lazy val propertiesFull: DataFrame = dataSourceProvider.readGold(ProcessedTables.PROPERTIES_FULL)
  }

  override protected def build(): DataFrame =
    sources.propertiesFull
      .groupBy("province", "operation")
      .agg(
        count("id").as("property_count"),
        round(avg(col("pool").cast(IntegerType)), 4).as("pool_ratio"),
        round(avg(col("garage").cast(IntegerType)), 4).as("garage_ratio"),
        round(avg(col("garden").cast(IntegerType)), 4).as("garden_ratio"),
        round(avg(col("terrace").cast(IntegerType)), 4).as("terrace_ratio"),
        round(avg(col("elevator").cast(IntegerType)), 4).as("elevator_ratio")
      )

}
