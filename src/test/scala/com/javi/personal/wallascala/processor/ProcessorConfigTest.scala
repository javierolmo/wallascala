package com.javi.personal.wallascala.processor

import com.javi.personal.wallascala.WallaScalaException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class ProcessorConfigTest extends AnyFlatSpec with Matchers {

  "parse" should "Parse date with format yyyy-M-d correctly" in {
    val args = Array(
      "--datasetName", "ANY_VALUE",
      "--date", "2021-1-2"
    )

    val result = ProcessorConfig.parse(args)

    result.date shouldEqual LocalDate.of(2021, 1, 2)
  }

  it should "Parse date with format yyyy-MM-dd correctly" in {
    val args = Array(
      "--datasetName", "ANY_VALUE",
      "--date", "2021-01-02"
    )

    val result = ProcessorConfig.parse(args)

    result.date shouldEqual LocalDate.of(2021, 1, 2)
  }

  it should "Parse datasetName correctly" in {
    val args = Array(
      "--datasetName", "SOME_VALUE",
      "--date", "2000-01-01"
    )

    val result = ProcessorConfig.parse(args)

    result.datasetName shouldEqual "SOME_VALUE"
  }

  it should "Fail if datasetName is not provided" in {
    val args = Array(
      "--date", "2000-01-01"
    )

    assertThrows[WallaScalaException] {
      ProcessorConfig.parse(args)
    }
  }

  it should "Fail if date is not provided" in {
    val args = Array(
      "--datasetName", "SOME_VALUE"
    )

    assertThrows[WallaScalaException] {
      ProcessorConfig.parse(args)
    }
  }

}
