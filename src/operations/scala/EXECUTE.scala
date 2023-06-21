import com.javi.personal.wallascala.cleaner.Cleaner
import com.javi.personal.wallascala.egestor.Egestor
import com.javi.personal.wallascala.ingestion.Ingestor
import com.javi.personal.wallascala.processor.Processor
import com.javi.personal.wallascala.services.impl.blob.SparkSessionFactory
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

  "CLEANER" should "CLEAN DATA" in {
    val cleaner = new Cleaner(spark)
    val from = LocalDate.of(2023, 6, 4)
    val to = LocalDate.now()

    val days: Int = ChronoUnit.DAYS.between(from, to).toInt
    val localDates: Seq[LocalDate] = (0 to days).map(x => from.plusDays(x))

    localDates.foreach(cleaner.execute("wallapop", "properties", _))
  }

  "PROCESSOR" should "PROCESS DATA" in {
    val processor = new Processor(spark)
    processor.process("properties")
    processor.process("price_changes")
  }

  "EGESTOR" should "EGEST DATA" in {
    val egestor: Egestor = new Egestor(spark)
    //egestor.writeProperties()
    egestor.writePriceChanges()
  }

}
