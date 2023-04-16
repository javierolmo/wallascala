package loads

import com.javi.personal.wallascala.extractor.Extractor
import com.javi.personal.wallascala.extractor.wallapop.WallapopApiExtractor
import com.javi.personal.wallascala.services.{BlobService, SecretService}
import org.scalatest.flatspec.AnyFlatSpec

class ExtractorExecutor extends AnyFlatSpec {

  "Extractor" should "extract data to raw folder" in {
    val extractor: Extractor = new WallapopApiExtractor(BlobService(SecretService()))
    extractor.extract()
  }

}
