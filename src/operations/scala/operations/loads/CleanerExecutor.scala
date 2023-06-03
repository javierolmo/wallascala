package operations.loads

import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.cleaner.Cleaner
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import operations.Operation
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

@Operation
class CleanerExecutor extends AnyFlatSpec {

  "Cleaner" should "clean data" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val cleaner = new Cleaner(blobService)
    cleaner.execute(DataCatalog.PISO_WALLAPOP, LocalDate.of(2023, 5, 14))
    cleaner.execute(DataCatalog.PISO_WALLAPOP, LocalDate.of(2023, 5, 15))
  }

}
