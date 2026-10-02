package com.javi.personal.tfg.processors.utils

import com.javi.personal.tfg.processors.utils.DataFrameOps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataFrameOpsTest extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DataFrameOpsTest")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  it should "align DataFrame with target schema filling missing columns with null and reordering" in {
    val inputDf = Seq(
      ("1", "Title 1")
    ).toDF("id", "title")

    val targetSchema = StructType(Array(
      StructField("id", StringType),
      StructField("missing_col", IntegerType),
      StructField("title", StringType),
      StructField("another_missing", BooleanType)
    ))

    val alignedDf = inputDf.alignWithSchema(targetSchema)

    alignedDf.schema shouldEqual targetSchema

    val row = alignedDf.first()
    row.getAs[String]("id") shouldEqual "1"
    row.isNullAt(alignedDf.schema.fieldIndex("missing_col")) shouldEqual true
    row.getAs[String]("title") shouldEqual "Title 1"
    row.isNullAt(alignedDf.schema.fieldIndex("another_missing")) shouldEqual true
  }

}
