package com.javi.personal.wallascala.cleaner

import com.javi.personal.wallascala.cleaner.FieldCleaner.{ErrorStruct, castField, createErrorStruct}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, lit, struct, trim, when}
import org.apache.spark.sql.types._

case class FieldCleaner(
                         name: String,
                         dataType: DataType,
                         filter: Option[Column => Column] = None,
                         transform: Option[Column => Column] = None,
                         defaultValue: Option[Any] = None) {

  def clean(inputField: Column): (Column, Column) = {
    val defaultedField = defaultValue.map(value => when(inputField.isNull, lit(value)).otherwise(inputField)).getOrElse(inputField)
    val excludedByFilter = filter.map(!_.apply(defaultedField)).getOrElse(lit(false))
    val transformedField = transform.map(_(defaultedField)).getOrElse(defaultedField)
    val castedField = transformedField.try_cast(dataType)

    val inputWasNonEmpty = inputField.isNotNull and (trim(inputField.cast(StringType)) =!= lit(""))
    val transformedIsNonEmpty = transformedField.isNotNull and (trim(transformedField.cast(StringType)) =!= lit(""))

    val errorCasting = inputWasNonEmpty and (transformedField.isNull or (transformedIsNonEmpty and castedField.isNull))
    val rightSide = when(!errorCasting and !excludedByFilter, castedField)
    val leftSide = array(
      when(errorCasting, createErrorStruct(inputField, name, dataType, "Error casting")).otherwise(lit(null).cast(ErrorStruct)),
      when(excludedByFilter, createErrorStruct(inputField, name, dataType, "Does not match filter")).otherwise(lit(null).cast(ErrorStruct))
    )
    (leftSide, rightSide)
  }

}

object FieldCleaner {

  private val FieldName = "fieldName"
  private val FieldValue = "fieldValue"
  private val FieldType = "fieldType"
  private val Message = "message"
  val ErrorStruct = StructType(Seq(
    StructField(FieldName, StringType),
    StructField(FieldValue, StringType),
    StructField(FieldType, StringType),
    StructField(Message, StringType)
  ))

  private def createErrorStruct(inputField: Column, fieldName: String, fieldType: DataType, message: String): Column =
    struct(
      lit(fieldName).as(FieldName),
      inputField.as(FieldValue),
      lit(fieldType.getClass.getSimpleName).as(FieldType),
      lit(message).as(Message)
    ).cast(ErrorStruct)

  private def castField(inputField: Column, dataType: DataType, function: Option[Column => Column]): Column =
    function.map(_(inputField)).getOrElse(inputField).try_cast(dataType)

}
