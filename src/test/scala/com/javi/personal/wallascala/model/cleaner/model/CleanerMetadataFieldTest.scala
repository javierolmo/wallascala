package com.javi.personal.wallascala.model.cleaner.model

import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class CleanerMetadataFieldTest extends AnyFlatSpec {

  "CleanerMetadataField." should "" in {
    val cleanerMetadataField = CleanerMetadataField("fieldName", "epoch", defaultValue = None, equalTo = None)
    import cleanerMetadataField._

    val result = cleanerMetadataField.genericValidation[Timestamp](null)

    assert(result == None)
  }

}
