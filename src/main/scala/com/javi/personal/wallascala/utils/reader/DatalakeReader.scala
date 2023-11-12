package com.javi.personal.wallascala.utils.reader
import com.javi.personal.wallascala.utils._
import com.javi.personal.wallascala.{PathBuilder, StorageAccountLocation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatalakeReader(layer: Layer, table: String, source: String = "") extends Reader {

  def format(): String = "parquet"
  def options(): Map[String, String] = Map()
  def schema(): Option[StructType] = Option.empty

  private def location(): StorageAccountLocation = {
    require(layer != null)
    layer match {
      case Processed => PathBuilder.buildProcessedPath(table)
      case Sanited =>
        require(source.nonEmpty)
        PathBuilder.buildSanitedPath(source, table)
      case SanitedExcluded =>
        require(source.nonEmpty)
        PathBuilder.buildExcludedPath(source, table)
      case Staging =>
        require(source.nonEmpty)
        PathBuilder.buildStagingPath(source, table)
      case Raw =>
        require(source.nonEmpty)
        PathBuilder.buildRawPath(source, table)
    }
  }

  override def read()(implicit spark: SparkSession): DataFrame = {
    val dfReader = spark.read
    val readerWithFormat = dfReader.format(format())
    val readerWithSchema = if (schema().isDefined) readerWithFormat.schema(schema().get) else readerWithFormat
    val readerWithOptions = readerWithSchema.options(options())
    readerWithOptions.load(location().url)
  }

}
