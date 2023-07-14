import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.cleaner.{Cleaner, CleanerCLI}
import com.javi.personal.wallascala.egestor.Egestor
import com.javi.personal.wallascala.ingestion.Ingestor
import com.javi.personal.wallascala.processor.{PriceChangesProcessor, Processor}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class EXECUTE extends AnyFlatSpec {

  private val spark: SparkSession = SparkSessionFactory.build()

  "INGESTOR" should "INGEST DATA" in {
    val ingestor = new Ingestor(spark)
    ingestor.ingest("wallapop", "properties")
  }

  "CLEANER" should "CLEAN SPECIFIC DATE" in {
    CleanerCLI.main(Array("--source", "wallapop", "--datasetName", "properties", "--date", "2023-07-14"))
  }

  "CLEANER" should "CLEAN DATA RANGE" in {
    val cleaner = new Cleaner(spark)
    val from = LocalDate.of(2023, 6, 19)
    val to = LocalDate.now()

    val days: Int = ChronoUnit.DAYS.between(from, to).toInt
    val localDates: Seq[LocalDate] = (0 to days).map(x => from.plusDays(x))

    localDates.foreach(cleaner.execute("wallapop", "properties", _))
  }

  "PROCESSOR" should "PROCESS PROPERTIES" in {
    Processor.execute("properties")
  }

  "PROCESSOR" should "PROCESS PRICE CHANGES" in {
    Processor.execute("price_changes")
  }

  "EGESTOR" should "EGEST DATA" in {
    val egestor: Egestor = new Egestor(spark)
    //egestor.writeProperties()
    egestor.writePriceChanges()
  }

}
