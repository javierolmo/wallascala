package com.javi.personal.wallascala.launcher

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class LauncherCLITest extends AnyFlatSpec with Matchers {

  "LauncherCLI.parse" should "correctly parse command line arguments" in {
    val args = Array(
      "-s", "source/path",
      "-t", "target/path",
      "--sourceFormat", "csv",
      "--targetFormat", "parquet",
      "--select", "field1,field2"
    )

    val result = LauncherConfig.parse(args)
  }

  it should "handle missing required arguments" in {
    val args = Array("-s", "source/path")

    val result = LauncherConfig.parse(args)
  }
}
