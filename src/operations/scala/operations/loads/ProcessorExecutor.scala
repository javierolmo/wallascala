package operations.loads

import com.javi.personal.wallascala.model.processor.Processor
import com.javi.personal.wallascala.model.services.{BlobService, SecretService}
import org.scalatest.flatspec.AnyFlatSpec

class ProcessorExecutor extends AnyFlatSpec {

  "Processor" should "process data" in {
    val secretService = SecretService()
    val blobService = BlobService(secretService)
    val processor = new Processor(blobService)
    processor.process("properties")
    processor.process("price_changes")
  }

}
