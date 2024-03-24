package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.cleaner.model.{CleanerMetadata, MetadataCatalog}
import com.javi.personal.wallascala.utils.reader.SparkFileReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CleanerTest extends AnyFlatSpec with Matchers with MockFactory {

  /* TODO
  implicit val spark: SparkSession = SparkSessionFactory.build()

  "Cleaner.execute" should "process data correctly" in {
    val mockSparkFileReader = mock[SparkFileReader]
    val mockDataFrame = spark.emptyDataFrame
    val metadataCatalog = MetadataCatalog(Seq(
      CleanerMetadata("test_metadata", Seq(
        FieldCleaner("field1", StringType),
        FieldCleaner("field2", IntegerType)
      ))
    ))
    val cleanerConfig = CleanerConfig(
        id = "test_metadata",
        sourcePath = "SOME_PATH_1",
        targetPath = "SOME_PATH_2",
        targetPathExclusions = "SOME_PATH_3"
    )
    (mockSparkFileReader.read _).expects().returning(mockDataFrame)

    Cleaner.execute(cleanerConfig, metadataCatalog)(spark)
  }
   */
}
