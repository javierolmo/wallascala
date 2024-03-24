package com.javi.personal.wallascala.cleaner.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MetadataCatalogTest extends AnyFlatSpec with Matchers {

  it should "find element by id" in {
    val metadata1 = CleanerMetadata("1", Seq())
    val catalog = MetadataCatalog(Seq(
      metadata1
    ))

    val result = catalog.findByCatalogItem("1")

    result.get should be (metadata1)
  }

  it should "throw exception when id is not present" in {
    val catalog = MetadataCatalog(Seq())

    val result = catalog.findByCatalogItem("1")

    result should be (None)
  }

  it should "get all elements in catalog" in {
    val metadata1 = CleanerMetadata("1", Seq())
    val metadata2 = CleanerMetadata("2", Seq())
    val catalog = MetadataCatalog(Seq(
      metadata1,
      metadata2
    ))

    val result = catalog.availableIds()

    result should contain theSameElementsAs Seq("1", "2")
  }

}
