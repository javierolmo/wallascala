package com.javi.personal.wallascala.processor.transformers

import com.javi.personal.wallascala.processor.etls.WallapopProperties._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, to_date}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

case class WallapopSources(sanitedWallapop: DataFrame, provinces: DataFrame)

object WallapopTransformer extends Transformer[WallapopSources, DataFrame] {

  override def transform(sources: WallapopSources): DataFrame =
    transform(sources.sanitedWallapop, sources.provinces)

  def transform(sanitedWallapop: DataFrame, provinces: DataFrame): DataFrame = {
    sanitedWallapop
      .withColumn("province_code", (col("location__postal_code").cast(IntegerType) / 1000).cast(IntegerType))
      .join(provinces.as("p"), col("province_code") === provinces("codigo").cast(IntegerType), "left")
      .withColumnRenamed("id", Id)
      .withColumnRenamed("title", Title)
      .withColumnRenamed("price__amount", Price)
      .withColumnRenamed("type_attributes__surface", Surface)
      .withColumnRenamed("type_attributes__rooms", Rooms)
      .withColumnRenamed("type_attributes__bathrooms", Bathrooms)
      .withColumnRenamed("location__city", City)
      .withColumnRenamed("location__country_code", Country)
      .withColumnRenamed("location__postal_code", PostalCode)
      .withColumnRenamed("location__region", Region)
      .withColumnRenamed("provincia", Province)
      .withColumnRenamed("type_attributes__operation", Operation)
      .withColumnRenamed("type_attributes__type", Type)
      .withColumnRenamed("description", Description)
      .withColumn(ModificationDate, to_date(col("modified_at")))
      .withColumn(Source, lit("wallapop"))
      .withColumn(Link, concat(lit("https://es.wallapop.com/item/"), col("web_slug")))
      .withColumn(CreationDate, to_date(col("created_at")))
      .withColumn(Elevator, lit(null).cast(BooleanType))
      .withColumn(Garage, lit(null).cast(BooleanType))
      .withColumn(Garden, lit(null).cast(BooleanType))
      .withColumn(Pool, lit(null).cast(BooleanType))
      .withColumn(Terrace, lit(null).cast(BooleanType))
      .withColumnRenamed("location__latitude", Latitude)
      .withColumnRenamed("location__longitude", Longitude)
      .dropDuplicates(Title, Price, Description, Surface, Operation)
  }

}
