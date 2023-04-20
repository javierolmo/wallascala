package operations.review

import com.javi.personal.wallascala.catalog.DataCatalog
import com.javi.personal.wallascala.services.impl.blob.model.ReadConfig
import com.javi.personal.wallascala.services.{BlobService, SecretService}
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec

@Ignore
class ExploreSanited extends AnyFlatSpec {

  "Explorer" should "read pisos" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val location = DataCatalog.PISO.sanitedLocation
    val df = blobService.read(location, config = ReadConfig(format = "parquet"))
    df.show()
  }

}
