package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.model.Transformations
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarRow
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.not.be
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

class FieldCleanerTest extends AnyFlatSpec {

  private val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "clean string correctly" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some("some_value")
    dataType shouldEqual StringType
  }

  it should "clean integer correctly" in {
    val input: String = "123123"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(123123)
    dataType shouldEqual IntegerType
  }

  it should "clean integer error when input is not an integer" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual IntegerType
  }

  it should "Default value should be taken when input field is null" in {
    val input: String = null
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(0)
    dataType shouldEqual IntegerType
  }

  it should "Default value should not be taken when cast fails" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual IntegerType
  }

  it should "Transformation should be applied before cast" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, transform = Some(Transformations.removeNonNumeric))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some(123)
    dataType shouldEqual IntegerType
  }

  it should "Filter should be applied before cast" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isin("some_value")))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual Some("some_value")
    dataType shouldEqual StringType
  }

  it should "Error should be returned when filter does not match (string comparation)" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isin("some_other_value")))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual StringType
  }

  it should "Error should be returned filter does not match (null case)" in {
    val input: String = null
    val cleaner = FieldCleaner("some_field", StringType, filter = Some(_.isNotNull))

    val (dataType, value) = executeCleaner(input, cleaner)

    value shouldEqual None
    dataType shouldEqual StringType
  }

  private def executeCleaner(input: String, cleaner: FieldCleaner): (DataType, Option[Any]) = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val (errors, result) = cleaner.clean(col("some_field"))
    val cleanedDF = df.withColumn("errors", errors).withColumn("result", result)
    (
      cleanedDF.schema("result").dataType,
      cleanedDF.select("result").collect().headOption.map(_.getAs[Any](0)).flatMap(Option(_))
    )
  }

}
