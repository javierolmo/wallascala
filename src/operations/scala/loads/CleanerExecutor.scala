package loads

import com.javi.personal.wallascala.catalog.DataCatalog
import com.javi.personal.wallascala.cleaner.Cleaner
import com.javi.personal.wallascala.services.{BlobService, SecretService}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

class CleanerExecutor extends AnyFlatSpec {

  "Cleaner" should "clean data" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val cleaner = new Cleaner(blobService)
    cleaner.execute(DataCatalog.PISO, LocalDate.now())
  }

}
