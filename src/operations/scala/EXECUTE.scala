import com.javi.personal.wallascala.processor.{ProcessorConfig, ProcessorOperations}
import com.javi.personal.wallascala.{PathBuilder, SparkSessionFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

@Ignore
class EXECUTE extends AnyFlatSpec {

  private implicit val spark: SparkSession = SparkSessionFactory.build()

  "SPARK" should  "CREATE DATABASES" in {
    spark.read
      .format("parquet")
      .load(PathBuilder.buildProcessedPath("properties_2").cd(LocalDate.of(2023, 12, 2)).url)
      .show(1000)
  }

  "SPARK" should "VERIFY IF DATABASE EXISTS" in {
    assert(spark.catalog.databaseExists("processed"))
  }

  "SPARK" should "INGEST PROVICENS" in {
    val df = spark.read.format("json").load(PathBuilder.buildStagingPath("opendatasoft", "provincias-espanolas.json").url)
    df
  }
}
