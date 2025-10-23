package com.javi.personal.wallascala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SampleIntegrationTest extends AnyFlatSpec with Matchers {

  "Sample integration test" should "fail intentionally" in {
    // This test intentionally fails to verify that the build continues
    // despite having a failing integration test
    fail("This integration test is intentionally failing to verify build continues")
  }

}
