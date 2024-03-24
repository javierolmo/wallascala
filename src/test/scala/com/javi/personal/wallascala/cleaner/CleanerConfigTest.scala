package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.WallaScalaException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CleanerConfigTest extends AnyFlatSpec {

  it should "parse arguments successfully" in {
    val args = Array(
      "--sourcePath", "SOME_SOURCE_PATH",
      "--targetPath", "SOME_TARGET_PATH",
      "--targetPathExclusions", "SOME_TARGET_PATH_EXCLUSIONS",
      "--id", "SOME_ID"
    )

    val result = CleanerConfig.parse(args)

    result.sourcePath shouldEqual "SOME_SOURCE_PATH"
    result.targetPath shouldEqual "SOME_TARGET_PATH"
    result.targetPathExclusions shouldEqual "SOME_TARGET_PATH_EXCLUSIONS"
    result.id shouldEqual "SOME_ID"
  }

  it should "Fail when one required argument is missing" in {
    val args = Array(
      "--targetPath", "SOME_TARGET_PATH",
      "--targetPathExclusions", "SOME_TARGET_PATH_EXCLUSIONS",
      "--id", "SOME_ID"
    )

    intercept[WallaScalaException] {
      CleanerConfig.parse(args)
    }
  }

}
