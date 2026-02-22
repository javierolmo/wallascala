package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.StructType

import java.sql.Date
import java.time.format.DateTimeFormatter

case class PropertiesFull(
                           id: String,
                           title: String,
                           price: Integer,
                           surface: Integer,
                           rooms: Integer,
                           bathrooms: Integer,
                           link: String,
                           source: String,
                           creation_date: String,
                           elevator: Boolean,
                           garage: Boolean,
                           garden: Boolean,
                           city: String,
                           country: String,
                           postal_code: Integer,
                           province: String,
                           region: String,
                           modification_date: Date,
                           operation: String,
                           pool: Boolean,
                           description: String,
                           terrace: Boolean,
                           `type`: String,
                           latitude: Double,
                           longitude: Double
)

@ETL(table = ProcessedTables.PROPERTIES_FULL)
class PropertiesFullProcessor(config: ProcessorConfig, dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider())(implicit spark: SparkSession) extends Processor(config, dataSourceProvider) {

  import org.apache.spark.sql.Encoders._
  // Encoder implicito disponible
  implicit val propertiesFullEncoder: Encoder[PropertiesFull] = Encoders.product[PropertiesFull]

  override protected val schema: StructType = propertiesFullEncoder.schema

  private def emptyDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  private object sources {
    private val date = config.date
    private val dateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    
    lazy val wallapopProperties: Dataset[PropertiesFull] =
      dataSourceProvider.readGoldOption(ProcessedTables.WALLAPOP_PROPERTIES)
        .getOrElse(emptyDataFrame)
        .withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy(col("modification_date").desc)))
        .filter(col("row_number") === 1)
        .select(schema.fields.map(f => col(f.name)): _*)
        .as[PropertiesFull]

    lazy val pisosProperties: Dataset[PropertiesFull] =
      dataSourceProvider.readGoldOption(ProcessedTables.PISOS_PROPERTIES)
        .getOrElse(emptyDataFrame)
        .withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy(col("modification_date").desc)))
        .filter(col("row_number") === 1)
        .select(schema.fields.map(f => col(f.name)): _*)
        .as[PropertiesFull]
  }

  override protected def build(): DataFrame = {
    sources.wallapopProperties
      .union(sources.pisosProperties)
      .toDF()
  }

}
