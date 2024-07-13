package com.javi.personal.wallascala.launcher

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class LauncherCLITest extends AnyFlatSpec with Matchers {

  "LauncherCLI.parse" should "correctly parse command line arguments" in {
    val args = Array(
      "--sourcePath", "SOME_SOURCE_PATH",
      "--sourceFormat", "csv",
      "--targetPath", "SOME_TARGET_PATH",
      "--targetFormat", "parquet",
      "--select", "field1,field2",
      "--coalesce", "1",
      "--flattenFields",
      "--addColumns", "field1=a,field2=b",
      "--mode", "overwrite"
    )

    val result = LauncherConfig.parse(args)

    result.sourcePath.get shouldEqual "SOME_SOURCE_PATH"
    result.sourceFormat shouldEqual "csv"
    result.targetPath.get shouldEqual "SOME_TARGET_PATH"
    result.targetFormat shouldEqual "parquet"
    result.select.get shouldEqual Seq("field1", "field2")
    result.coalesce.get shouldEqual 1
    result.flattenFields shouldEqual true
    result.newColumns shouldEqual Seq("field1=a", "field2=b")
    result.mode.get shouldEqual "overwrite"
  }

  it should "handle missing required arguments" in {
    val args = Array("--sourcePath", "source/path")

    val result = LauncherConfig.parse(args)

    result.sourcePath.get shouldEqual "source/path"}
}
