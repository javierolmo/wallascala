package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.SparkSessionFactory
import com.javi.personal.wallascala.cleaner.model.Transformations
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.not.be
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

class FieldCleanerTest extends AnyFlatSpec {

  private val spark = SparkSessionFactory.build()

  import spark.implicits._

  it should "clean string correctly" in {
    val input: String = "some_value"
    val cleaner = FieldCleaner("some_field", StringType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value should be ("some_value")
    dataType should equal (StringType)
  }

  it should "clean integer correctly" in {
    val input: String = "123123"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value should be (123123)
    dataType should equal (IntegerType)
  }

  it should "clean integer error when input is not an integer" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType)

    val (dataType, value) = executeCleaner(input, cleaner)

    value should equal (None)
    dataType should equal (IntegerType)
  }

  it should "Default value should be taken when input field is null" in {
    val input: String = null
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value should be (0)
    dataType should equal (IntegerType)
  }

  it should "Default value should not be taken when cast fails" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, defaultValue = Some(0))

    val (dataType, value) = executeCleaner(input, cleaner)

    value should equal (None)
    dataType should equal (IntegerType)
  }

  it should "Transformation should be applied before cast" in {
    val input: String = "sd123gasd"
    val cleaner = FieldCleaner("some_field", IntegerType, transform = Some(Transformations.removeNonNumeric))

    val (dataType, value) = executeCleaner(input, cleaner)

    value should be (123)
    dataType should equal (IntegerType)
  }

  private def executeCleaner(input: String, cleaner: FieldCleaner): (DataType, Any) = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val cleanedDF = df.select(cleaner.clean(col("some_field")))
    val result: Row = cleanedDF.collect()(0)
    (
      result.schema.fields(0).dataType.asInstanceOf[StructType].fields(0).dataType,
      Option(result(0).asInstanceOf[GenericRowWithSchema].get(0))
    )
  }

}
