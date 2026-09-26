package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.processor.etls.WallapopProperties
import com.javi.personal.wallascala.utils.DataSourceProvider
import com.javi.personal.wallascala.utils.writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class ProcessorTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ProcessorTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  class TestWriter extends SparkWriter() {
    var written: Boolean = false
    override def write(dataFrame: DataFrame)(implicit spark: SparkSession): Unit = {
      written = true
    }
  }

  class StubDataSourceProvider(val wallapopDf: DataFrame, val provincesDf: DataFrame) extends DataSourceProvider {
    override def readSilver(source: String, datasetName: String)(implicit spark: SparkSession): DataFrame = {
      if (source == "opendatasoft") provincesDf else spark.emptyDataFrame
    }
    override def readSilver(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): DataFrame = {
      if (source == "wallapop") wallapopDf else spark.emptyDataFrame
    }
    override def readSilverOption(source: String, datasetName: String, date: LocalDate)(implicit spark: SparkSession): Option[DataFrame] = None
    override def readGold(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): DataFrame = spark.emptyDataFrame
    override def readGoldOption(dataset: ProcessedTables, dateOption: Option[LocalDate])(implicit spark: SparkSession): Option[DataFrame] = None
  }

  it should "execute processor using customWriter and return aligned DataFrame" in {
    val wallapopDf = Seq(
      ("item-1", "Test item", 100000, 70, 2, 1, "Madrid", "ES", 28001, "Madrid", "sale", "flat", "Desc", "2024-01-01", "slug-1", "2024-01-01", 40.0, -3.0)
    ).toDF(
      "id",
      "title",
      "price__amount",
      "type_attributes__surface",
      "type_attributes__rooms",
      "type_attributes__bathrooms",
      "location__city",
      "location__country_code",
      "location__postal_code",
      "location__region",
      "type_attributes__operation",
      "type_attributes__type",
      "description",
      "modified_at",
      "web_slug",
      "created_at",
      "location__latitude",
      "location__longitude"
    )

    val provincesDf = Seq((28, "Madrid")).toDF("codigo", "provincia")

    val stubProvider = new StubDataSourceProvider(wallapopDf, provincesDf)
    val testWriter = new TestWriter()
    val config = ProcessorConfig(ProcessedTables.WALLAPOP_PROPERTIES, LocalDate.of(2024, 1, 1), "dummy/path")

    val processor = new WallapopProperties(config, stubProvider, Some(testWriter))
    val resultDf = processor.execute()

    testWriter.written shouldEqual true
    resultDf.count() shouldEqual 1
    resultDf.columns should contain ("id")
    resultDf.columns should contain ("link")
    resultDf.first().getAs[String]("id") shouldEqual "item-1"
  }

}
