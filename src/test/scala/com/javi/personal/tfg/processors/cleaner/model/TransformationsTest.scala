package com.javi.personal.tfg.processors.cleaner.model

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class TransformationsTest extends AnyFlatSpec with Matchers {

  private val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  "removeNonNumeric" should "keep only numbers" in {
    val input = "as125asf12"
    val transformation:Column => Column = Transformations.removeNonNumeric

    val value = executeTransformation(input, transformation)

    value should be (Some("12512"))
  }

  it should "remove line breaks" in {
    val input = "some\n value\n with\n line\n breaks"
    val transformation:Column => Column = Transformations.removeLineBreaks

    val value = executeTransformation(input, transformation)

    value should be (Some("some value with line breaks"))
  }

  "parseTimestamp" should "correctly parse 13-digit epoch milliseconds without overflow" in {
    val input = "1685952314128"
    val transformation: Column => Column = Transformations.parseTimestamp

    val value = executeTransformation(input, transformation)

    value shouldBe defined
    val ts = value.get.asInstanceOf[Timestamp]
    val year = ts.toLocalDateTime.getYear
    year shouldEqual 2023
  }

  it should "correctly parse 10-digit epoch seconds" in {
    val input = "1685952314"
    val transformation: Column => Column = Transformations.parseTimestamp

    val value = executeTransformation(input, transformation)

    value shouldBe defined
    val ts = value.get.asInstanceOf[Timestamp]
    val year = ts.toLocalDateTime.getYear
    year shouldEqual 2023
  }

  private def executeTransformation(input: String, transformation: Column => Column): Option[Any] = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val cleanedDF = df.select(transformation(col("some_field")))
    val result: Row = cleanedDF.collect()(0)
    Option(result(0))
  }

}
