package com.javi.personal.wallascala.cleaner.model

import com.javi.personal.wallascala.cleaner.FieldCleaner
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types._

case class MetadataCatalog(items: Seq[CleanerMetadata]) {

    def findByCatalogItem(id: String): Option[CleanerMetadata] = items.find(metadata => metadata.id == id)

    def availableIds(): Seq[String] = items.map(metadata => metadata.id)


}

object MetadataCatalog {

  def default(): MetadataCatalog = MetadataCatalog(
    items = Seq(
      wallapopPropertiesOld,
      wallapopProperties,
      fotocasaProperties,
      provinciasEspanolas,
      pisosProperties,
      zipBoundaries
    )
  )

  private val zipBoundaries: CleanerMetadata = CleanerMetadata(
    id = "zip_boundaries",
    fields = Seq(
      FieldCleaner("properties__CODIGO_INE", IntegerType),
      FieldCleaner("properties__COD_POSTAL", IntegerType, filter = Some(_.isNotNull)),
      FieldCleaner("geometry__coordinates", ArrayType(ArrayType(ArrayType(DoubleType))), filter = Some(_.isNotNull)),
    )
  )

  private val wallapopPropertiesOld: CleanerMetadata = CleanerMetadata(
    id = "wallapop_properties_old",
    fields = Seq(
      FieldCleaner("bathrooms", IntegerType),
      FieldCleaner("category_id", IntegerType, filter = Some(_.equalTo("200"))),
      FieldCleaner("condition", StringType),
      FieldCleaner("creation_date", TimestampType, transform = Some(Transformations.millisecondsToTimestamp)),
      FieldCleaner("currency", StringType),
      FieldCleaner("distance", DoubleType),
      FieldCleaner("elevator", BooleanType),
      FieldCleaner("favorited", BooleanType),
      FieldCleaner("flags__banned", BooleanType),
      FieldCleaner("flags__expired", BooleanType),
      FieldCleaner("flags__onhold", BooleanType),
      FieldCleaner("flags__pending", BooleanType),
      FieldCleaner("flags__reserved", BooleanType),
      FieldCleaner("flags__sold", BooleanType),
      FieldCleaner("garage", BooleanType),
      FieldCleaner("garden", BooleanType),
      FieldCleaner("id", StringType),
      FieldCleaner("images", StringType),
      FieldCleaner("location__city", StringType),
      FieldCleaner("location__country_code", StringType),
      FieldCleaner("location__postal_code", IntegerType),
      FieldCleaner("modification_date", TimestampType, transform = Some(Transformations.millisecondsToTimestamp)),
      FieldCleaner("operation", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("pool", BooleanType),
      FieldCleaner("price", DoubleType),
      FieldCleaner("rooms", IntegerType),
      FieldCleaner("storytelling", StringType),
      FieldCleaner("surface", IntegerType),
      FieldCleaner("terrace", BooleanType),
      FieldCleaner("title", StringType),
      FieldCleaner("type", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("user__id", StringType),
      FieldCleaner("visibility_flags__boosted", BooleanType),
      FieldCleaner("visibility_flags__bumped", BooleanType),
      FieldCleaner("visibility_flags__country_bumped", BooleanType),
      FieldCleaner("visibility_flags__highlighted", BooleanType),
      FieldCleaner("visibility_flags__urgent", BooleanType),
      FieldCleaner("web_slug", StringType),
      FieldCleaner("source", StringType),
      FieldCleaner("date", StringType),
    )
  )

  private val wallapopProperties: CleanerMetadata = CleanerMetadata(
    id = "wallapop_properties",
    fields = Seq(
      FieldCleaner("bathrooms", IntegerType),
      FieldCleaner("category_id", IntegerType, filter = Some(_.equalTo("200"))),
      FieldCleaner("condition", StringType),
      FieldCleaner("creation_date", TimestampType, transform = Some(to_timestamp)),
      FieldCleaner("currency", StringType),
      FieldCleaner("distance", DoubleType),
      FieldCleaner("elevator", BooleanType),
      FieldCleaner("favorited", BooleanType),
      FieldCleaner("flags__banned", BooleanType),
      FieldCleaner("flags__expired", BooleanType),
      FieldCleaner("flags__onhold", BooleanType),
      FieldCleaner("flags__pending", BooleanType),
      FieldCleaner("flags__reserved", BooleanType),
      FieldCleaner("flags__sold", BooleanType),
      FieldCleaner("garage", BooleanType),
      FieldCleaner("garden", BooleanType),
      FieldCleaner("id", StringType),
      FieldCleaner("images", StringType),
      FieldCleaner("location__city", StringType),
      FieldCleaner("location__country_code", StringType),
      FieldCleaner("location__postal_code", IntegerType),
      FieldCleaner("modification_date", TimestampType, transform = Some(to_timestamp)),
      FieldCleaner("operation", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("pool", BooleanType),
      FieldCleaner("price", DoubleType),
      FieldCleaner("rooms", IntegerType),
      FieldCleaner("storytelling", StringType),
      FieldCleaner("surface", IntegerType),
      FieldCleaner("terrace", BooleanType),
      FieldCleaner("title", StringType),
      FieldCleaner("type", StringType, filter = Some(_.isNotNull)),
      FieldCleaner("user__id", StringType),
      FieldCleaner("visibility_flags__boosted", BooleanType),
      FieldCleaner("visibility_flags__bumped", BooleanType),
      FieldCleaner("visibility_flags__country_bumped", BooleanType),
      FieldCleaner("visibility_flags__highlighted", BooleanType),
      FieldCleaner("visibility_flags__urgent", BooleanType),
      FieldCleaner("web_slug", StringType),
      FieldCleaner("source", StringType),
      FieldCleaner("date", StringType),
    )
  )

  private val fotocasaProperties: CleanerMetadata = CleanerMetadata(
    id = "fotocasa_properties",
    fields = Seq(
      FieldCleaner("address", StringType),
      FieldCleaner("description", StringType),
      FieldCleaner("features__bathrooms", IntegerType),
      FieldCleaner("features__floor", IntegerType),
      FieldCleaner("features__rooms", IntegerType),
      FieldCleaner("features__size", IntegerType),
      FieldCleaner("id", IntegerType),
      FieldCleaner("price", IntegerType),
      FieldCleaner("timeAgo", IntegerType),
      FieldCleaner("title", StringType),
      FieldCleaner("url", StringType),
      FieldCleaner("city", StringType),
      FieldCleaner("operation", StringType),
      FieldCleaner("type", StringType)
    )
  )

  private val pisosProperties: CleanerMetadata = CleanerMetadata(
    id = "pisos_properties",
    fields = Seq(
      FieldCleaner("id", StringType),
      FieldCleaner("title", StringType, transform = Some(Transformations.removeLineBreaks)),
      FieldCleaner("price", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("url", StringType),
      FieldCleaner("description", StringType, transform = Some(Transformations.removeLineBreaks)),
      FieldCleaner("address", StringType),
      FieldCleaner("rooms", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("bathrooms", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("size", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("floor", IntegerType, transform = Some(Transformations.removeNonNumeric)),
      FieldCleaner("city", StringType),
      FieldCleaner("operation", StringType),
      FieldCleaner("type", StringType)
    )
  )

  private val provinciasEspanolas: CleanerMetadata = CleanerMetadata(
    id = "opendatasoft_provincias-espanolas",
    fields = Seq(
      FieldCleaner("ccaa", StringType),
      FieldCleaner("cod_ccaa", IntegerType),
      FieldCleaner("codigo", IntegerType),
      FieldCleaner("geo_point_2d", StructType(Seq(
        StructField("lat", DoubleType),
        StructField("lon", DoubleType)
      ))),
      FieldCleaner("geo_shape", StructType(Seq(
        StructField("geometry", StructType(Seq(
          StructField("coordinates", ArrayType(ArrayType(ArrayType(StringType)))),
          StructField("type", DoubleType)
        ))),
        StructField("type", StringType)
      ))),
      FieldCleaner("provincia", StringType),
      FieldCleaner("texto", StringType),
    )
  )

}
