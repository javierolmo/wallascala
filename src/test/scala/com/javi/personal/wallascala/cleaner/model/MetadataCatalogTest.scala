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

  it should "contain fotocasa_properties in default catalog with correct fields" in {
    val catalog = MetadataCatalog.default()
    val fotocasa = catalog.findByCatalogItem("fotocasa_properties")

    fotocasa should be ('defined)
    catalog.findByCatalogItem("fotocasa_properties_old") should be (None)
    catalog.findByCatalogItem("fotocasa_properties_2") should be (None)
    val fieldNames = fotocasa.get.fields.map(_.name)
    fieldNames should contain theSameElementsAs Seq(
      "baños", "coordenadas__accuracy", "coordenadas__latitude", "coordenadas__longitude",
      "fecha_scraping", "habitaciones", "id", "metros", "municipio", "operacion",
      "precio", "provincia", "publicado_hace", "tipo_detalle", "tipo_inmueble",
      "ubicacion", "url"
    )
  }

}
