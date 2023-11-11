package com.javi.personal.wallascala.cleaner.model

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, struct, typedLit, when}
import org.apache.spark.sql.types._

case class CleanerMetadataField(name: String, dataType: DataType, transform: Option[Column => Column] = Option.empty, defaultValue: Option[Any] = Option.empty, equalTo: Option[AnyVal] = Option.empty) {

  def genericFieldCleaner(inputField: Column): Column = {
    val defaultedField = if(defaultValue.isDefined)
      when(inputField.isNull and lit(defaultValue.isDefined), lit(defaultValue.get)).otherwise(inputField)
    else
      inputField
    val nulledField = when(defaultedField === "null", null).otherwise(defaultedField)
    val castedField = castField(nulledField, dataType, transform)
    val errorCasting = when(inputField.isNotNull and castedField.isNull, lit(true)).otherwise(lit(false))
    val leftSide = when(!errorCasting, castedField)
    val rightSide = when(errorCasting, struct(
      lit(name).as("fieldName"),
      inputField.as("fieldValue"),
      lit(dataType.getClass.getSimpleName).as("fieldType"),
      lit("Error al castear").as("message")
    )).otherwise(
      typedLit[StructType](null)
    )
    struct(leftSide.as("result"), rightSide.as("error"))
  }

  private def castField(inputField: Column, dataType: DataType, function: Option[Column => Column]): Column = {
    val transformed = if (function.isDefined) function.get.apply(inputField) else inputField
    transformed.cast(dataType)
  }

}