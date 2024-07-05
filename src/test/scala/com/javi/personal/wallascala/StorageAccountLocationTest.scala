package com.javi.personal.wallascala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StorageAccountLocationTest extends AnyFlatSpec with Matchers {

  "cd(string)" should "return a new StorageAccountLocation with the path updated" in {
    val storageAccountLocation = StorageAccountLocation("account", "container", "path")
    val newStorageAccountLocation = storageAccountLocation.cd("newPath")
    newStorageAccountLocation.path shouldEqual "path/newPath"
  }

  "cd(localDate)" should "return a new StorageAccountLocation with the path updated" in {
    val storageAccountLocation = StorageAccountLocation("account", "container", "path")
    val newStorageAccountLocation = storageAccountLocation.cd(java.time.LocalDate.of(2021, 3, 5))
    newStorageAccountLocation.path shouldEqual "path/year=2021/month=3/day=5"
  }

}
