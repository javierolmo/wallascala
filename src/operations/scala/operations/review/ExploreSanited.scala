package operations.review

import com.javi.personal.wallascala.model.catalog.DataCatalog
import com.javi.personal.wallascala.model.services.impl.blob.model.ReadConfig
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.scalatest.flatspec.AnyFlatSpec

class ExploreSanited extends AnyFlatSpec {

  "Explorer" should "read pisos" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val location = DataCatalog.PISO_WALLAPOP.sanitedLocation
    val df = blobService.read(location, config = ReadConfig(format = "parquet"))
    df.write.saveAsTable("piso_wallapop")
    df.show()
  }

}
