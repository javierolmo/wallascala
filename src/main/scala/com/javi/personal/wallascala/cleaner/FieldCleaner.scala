package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.FieldCleaner.{castField, createErrorStruct}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, lit, struct, typedLit, when}
import org.apache.spark.sql.types._

case class FieldCleaner(
                         name: String,
                         dataType: DataType,
                         filter: Option[Column => Column] = None,
                         transform: Option[Column => Column] = None,
                         defaultValue: Option[Any] = None) {

  def clean(inputField: Column): (Column, Column) = {
    val nulledField = when(inputField === "null", null).otherwise(inputField)
    val defaultedField = defaultValue.map(value => when(nulledField.isNull, lit(value)).otherwise(nulledField)).getOrElse(nulledField)
    val excludedByFilter = filter.map(!_.apply(defaultedField)).getOrElse(lit(false))
    val castedField = castField(defaultedField, dataType, transform, filter)
    val errorCasting = inputField.isNotNull and castedField.isNull
    val rightSide = when(!errorCasting and !excludedByFilter, castedField)
    val leftSide = array(
      when(errorCasting, createErrorStruct(inputField, name, dataType, "Error casting")).otherwise(typedLit[StructType](null)),
      when(excludedByFilter, createErrorStruct(inputField, name, dataType, "Does not match filter " + excludedByFilter.expr.sql)).otherwise(typedLit[StructType](null))
    )
    (leftSide, rightSide)
  }

}

object FieldCleaner {

  private val FieldName = "fieldName"
  private val FieldValue = "fieldValue"
  private val FieldType = "fieldType"
  private val Message = "message"
  private val ErrorStruct = StructType(Seq(
    StructField(FieldName, StringType),
    StructField(FieldValue, StringType),
    StructField(FieldType, StringType),
    StructField(Message, StringType)
  ))

  private def createErrorStruct(inputField: Column, fieldName: String, fieldType: DataType, message: String): Column = {
    struct(
      lit(fieldName).as(FieldName),
      inputField.as(FieldValue),
      lit(fieldType.getClass.getSimpleName).as(FieldType),
      lit(message).as(Message)
    ).cast(ErrorStruct)
  }

  private def castField(inputField: Column, dataType: DataType, function: Option[Column => Column], filter: Option[Column => Column]): Column = {
    val transformed = function.map(_.apply(inputField)).getOrElse(inputField)
    transformed.cast(dataType)
  }

}