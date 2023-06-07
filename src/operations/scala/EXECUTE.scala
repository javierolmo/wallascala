import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.cleaner.Cleaner
import com.javi.personal.wallascala.model.egestor.Egestor
import com.javi.personal.wallascala.model.ingestion.Ingestor
import com.javi.personal.wallascala.model.processor.Processor
import com.javi.personal.wallascala.model.services.impl.blob.SparkSessionFactory
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

class EXECUTE extends AnyFlatSpec {

  private val spark: SparkSession = SparkSessionFactory.build()

  "INGESTOR" should "INGEST DATA" in {
    val ingestor = new Ingestor(spark)
    ingestor.ingest(DataCatalog.PISO_WALLAPOP)
  }

  "CLEANER" should "CLEAN DATA" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val cleaner = new Cleaner(blobService)
    cleaner.execute(DataCatalog.PISO_WALLAPOP, LocalDate.of(2023, 6, 4))
  }

  "PROCESSOR" should "PROCESS DATA" in {
    val processor = new Processor(spark)
    //processor.process("properties")
    processor.process("price_changes")
  }

  "EGESTOR" should "EGEST DATA" in {
    val egestor: Egestor = new Egestor(spark)
    //egestor.writeProperties()
    egestor.writePriceChanges()
  }

}
