package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.FieldCleaner.{castField, createErrorStruct}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, struct, typedLit, when}
import org.apache.spark.sql.types._

case class FieldCleaner(name: String, dataType: DataType, transform: Option[Column => Column] = Option.empty, defaultValue: Option[Any] = Option.empty) {

  def clean(inputField: Column): Column = {
    val defaultedField = defaultValue.map(value => when(inputField.isNull, lit(value)).otherwise(inputField)).getOrElse(inputField)
    val nulledField = when(defaultedField === "null", null).otherwise(defaultedField)
    val castedField = castField(nulledField, dataType, transform)
    val errorCasting = when(inputField.isNotNull and castedField.isNull, lit(true)).otherwise(lit(false))
    val leftSide = when(!errorCasting, castedField)
    val rightSide = when(errorCasting, createErrorStruct(inputField, name, dataType)).otherwise(typedLit[StructType](null))
    struct(leftSide.as("result"), rightSide.as("error"))
  }

}

object FieldCleaner {

  private def createErrorStruct(inputField: Column, fieldName: String, fieldType: DataType): Column = {
    struct(
      lit(fieldName).as("fieldName"),
      inputField.as("fieldValue"),
      lit(fieldType.getClass.getSimpleName).as("fieldType"),
      lit("Error al castear").as("message")
    )
  }

  private def castField(inputField: Column, dataType: DataType, function: Option[Column => Column]): Column = {
    val transformed = function.map(_.apply(inputField)).getOrElse(inputField)
    transformed.cast(dataType)
  }

}