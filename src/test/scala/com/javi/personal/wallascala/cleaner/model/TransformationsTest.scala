package com.javi.personal.wallascala.cleaner.model

import com.javi.personal.wallascala.SparkSessionFactory
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers.not.be

class TransformationsTest extends AnyFlatSpec {

  private val spark = SparkSessionFactory.build()

  import spark.implicits._

  "removeNonNumeric" should "keep only numbers" in {
    val input = "as125asf12"
    val transformation:Column => Column = Transformations.removeNonNumeric

    val value = executeTransformation(input, transformation)

    value should be (12512)
  }

  it should "" in {
    val input = "some\n value\n with\n line\n breaks"
    val transformation:Column => Column = Transformations.removeLineBreaks

    val value = executeTransformation(input, transformation)

    value should be ("some value with line breaks")
  }

  private def executeTransformation(input: String, transformation: Column => Column): Any = {
    val df: DataFrame = Seq(input).toDF("some_field")
    val cleanedDF = df.select(transformation(col("some_field")))
    val result: Row = cleanedDF.collect()(0)
    Option(result(0))
  }

}
