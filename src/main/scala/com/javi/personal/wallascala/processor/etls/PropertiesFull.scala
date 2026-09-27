package com.javi.personal.wallascala.processor.etls

import com.javi.personal.wallascala.processor.{ETL, ProcessedTables, Processor, ProcessorConfig}
import com.javi.personal.wallascala.utils.{DataSourceProvider, DefaultDataSourceProvider}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import java.sql.Date

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
                           longitude: Double,
                           load_date: Date
)

import com.javi.personal.wallascala.processor.transformers.PropertiesFullTransformer
import com.javi.personal.wallascala.utils.writers.SparkWriter

@ETL(table = ProcessedTables.PROPERTIES_FULL)
class PropertiesFullProcessor(
  config: ProcessorConfig,
  dataSourceProvider: DataSourceProvider = new DefaultDataSourceProvider(),
  customWriter: Option[SparkWriter] = None
)(implicit spark: SparkSession) extends Processor(config, dataSourceProvider, customWriter) {

  import org.apache.spark.sql.Encoders._
  // Encoder implicito disponible
  implicit val propertiesFullEncoder: Encoder[PropertiesFull] = Encoders.product[PropertiesFull]

  override protected val schema: StructType = propertiesFullEncoder.schema

  private def emptyDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  private object sources {
    lazy val wallapopProperties: DataFrame =
      dataSourceProvider.readGoldOption(ProcessedTables.WALLAPOP_PROPERTIES)
        .getOrElse(emptyDataFrame)

    lazy val pisosProperties: DataFrame =
      dataSourceProvider.readGoldOption(ProcessedTables.PISOS_PROPERTIES)
        .getOrElse(emptyDataFrame)

    lazy val fotocasaProperties: DataFrame =
      dataSourceProvider.readGoldOption(ProcessedTables.FOTOCASA_PROPERTIES)
        .getOrElse(emptyDataFrame)
  }

  override protected def build(): DataFrame = {
    PropertiesFullTransformer.transform(sources.wallapopProperties, sources.pisosProperties, sources.fotocasaProperties)
  }

}
