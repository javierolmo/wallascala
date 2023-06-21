package operations.review

import com.javi.personal.wallascala.services.impl.blob.model.{ReadConfig, StorageAccountLocation}
import com.javi.personal.wallascala.services.{BlobService, SecretService}
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec

class ExploreProcessed extends AnyFlatSpec {

  "Explorer" should "read pisos" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val location = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/price_changes",
      v2 = true
    )
    val df = blobService.read(location, config = ReadConfig(format = "delta"))

    df.orderBy(col("price_changes").desc).show(9999, false)
  }

}
