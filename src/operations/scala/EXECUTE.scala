import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.cleaner.{Cleaner, CleanerCLI, CleanerTest}
import com.javi.personal.wallascala.processor.{Processor, ProcessorCLI}
import com.javi.personal.wallascala.ingestion.Ingestor
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class EXECUTE extends AnyFlatSpec {

  private implicit val spark: SparkSession = SparkSessionFactory.build()

  "INGESTOR" should "INGEST DATA" in {
    val ingestor = new Ingestor(spark)
    ingestor.ingest(source = "wallapop", datasetName = "properties")
  }

  "CLEANER" should "CLEAN SPECIFIC DATE" in {
    CleanerCLI.main(Array("--source", "wallapop", "--datasetName", "properties", "--date", "2023-10-01"))
  }

  "CLEANER" should "CLEAN DATA RANGE" in {
    val cleaner = new Cleaner(spark)
    val from = LocalDate.of(2023, 10, 2)
    val to = LocalDate.now()

    val days: Int = ChronoUnit.DAYS.between(from, to).toInt
    val localDates: Seq[LocalDate] = (0 to days).map(x => from.plusDays(x))

    localDates.foreach(cleaner.execute("wallapop", "properties", _))
  }

  "PROCESSOR" should "PROCESS SOMETHING" in {
    ProcessorCLI.main(Array("--datasetName", "properties", "--date", "2023-10-01", "hola"))
  }

  "PROCESSOR" should "PROCESS PROPERTIES" in {
    Processor.properties(LocalDate.now()).execute()
  }

  "PROCESSOR" should "PROCESS PRICE CHANGES" in {
    Processor.priceChanges(LocalDate.now()).execute()
  }

  "PROCESSOR" should "PROCESS POSTAL CODE ANALYSIS" in {
    Processor.postalCodeAnalysis(LocalDate.now()).execute()
  }

  "SPARK" should  "CREATE DATABASES" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS processed")
  }

  "SPARK" should "VERIFY IF DATABASE EXISTS" in {
    assert(spark.catalog.databaseExists("processed"))
  }
}
