import com.javi.personal.wallascala.services.impl.blob.SparkSessionFactory
import com.javi.personal.wallascala.services.impl.blob.model.StorageAccountLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec

class EXPLORER extends AnyFlatSpec {

  private val spark: SparkSession = SparkSessionFactory.build()

  "EXPLORER" should "EXPLORE PROCESSED PROPERTIES" in {
    val location = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/properties",
      v2 = true
    )
    val df = spark.read.parquet(location.url)
      .groupBy("extracted_date").count()
      .orderBy(col("extracted_date").desc)
    df.show()
  }

  "EXPLORER" should "EXPLORE PROCESSED PRICE_CHANGES" in {
    val location = StorageAccountLocation(
      account = "melodiadl",
      container = "test",
      path = "processed/price_changes",
      v2 = true
    )
    val df = spark.read.parquet(location.url)
    df.show()
  }

}
