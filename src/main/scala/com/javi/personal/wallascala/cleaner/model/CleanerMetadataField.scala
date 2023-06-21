package com.javi.personal.wallascala.cleaner.model

import com.javi.personal.wallascala.cleaner.validator.FieldError
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

case class CleanerMetadataField(name: String, fieldType: String, defaultValue: Option[Any] = None, equalTo: Option[AnyVal] = None) {

  def buildUDF: UserDefinedFunction = fieldType.toLowerCase match {
    case "string" => udf(genericValidation[String](_: String))
    case "int" | "integer" => udf(genericValidation[Int](_: String))
    case "bool" | "boolean" => udf(genericValidation[Boolean](_: String))
    case "double" | "float" => udf(genericValidation[Double](_: String))
    case "epoch" => udf(genericValidation[Timestamp](_: String))
    case _ => println("Ey que no tienes la UDF adecuada"); null
  }

  def genericValidation[A <: Any](fieldValue: String)(implicit converter: (String, Option[A]) => Either[FieldError, Option[A]]): (Option[A], Option[FieldError]) = {
    val defaultValue = this.defaultValue.asInstanceOf[Option[A]]
    val equalTo = this.equalTo.asInstanceOf[Option[A]]
    converter(fieldValue, defaultValue) match {
      case Left(fieldError: FieldError) => (Option.empty, Some(fieldError))
      case Right(value) => {
        if (equalTo.isDefined) {
          if (equalTo.get.equals(value.get)) {
            (value, None)
          } else {
            val fieldError = FieldError(fieldName = name, fieldValue = fieldValue, fieldType = fieldType, message = s"Field value $fieldValue is not equal to $equalTo")
            (defaultValue, Some(fieldError))
          }
        } else {
          (value, None)
        }
      }
    }
  }

  implicit def stringToInt(s: String, defaultValue: Option[Int]): Either[FieldError, Option[Int]] = {
    try {
      if(s == null) Right(None) else Right(Option(s.toInt))
    } catch {
      case e: Exception =>
        if (defaultValue.isDefined) {
          Right(defaultValue)
        } else {
          Left(FieldError(fieldName = name, fieldValue = s, fieldType = fieldType, message = e.toString))
        }
    }
  }

  implicit def stringToBoolean(s: String, defaultValue: Option[Boolean]): Either[FieldError, Option[Boolean]] = {
    try {
      if(s == null) Right(None) else Right(Option(s.toBoolean))
    } catch {
      case e: Exception =>
        if (defaultValue.isDefined) {
          Right(defaultValue)
        } else {
          Left(FieldError(fieldName = name, fieldValue = s, fieldType = fieldType, message = e.toString))
        }
    }
  }

  implicit def stringToDouble(s: String, defaultValue: Option[Double]): Either[FieldError, Option[Double]] = {
    try {
      if(s == null) Right(None) else Right(Option(s.toDouble))
    } catch {
      case e: Exception =>
        if (defaultValue.isDefined) {
          Right(defaultValue)
        } else {
          Left(FieldError(fieldName = name, fieldValue = s, fieldType = fieldType, message = e.toString))
        }
    }
  }

  implicit def stringToString(s: String, defaultValue: Option[String]): Either[FieldError, Option[String]] = Right(Option(s))

  implicit def stringToEpoch(s: String, defaultValue: Option[Timestamp]): Either[FieldError, Option[Timestamp]] = {
    try {
      if(s == null) Right(None) else Right(Some(Timestamp.valueOf(LocalDateTime.ofEpochSecond(s.toLong / 1000, 0, ZoneOffset.UTC))))
    } catch {
      case e: Exception =>
        if (defaultValue.isDefined) {
          Right(defaultValue)
        } else {
          Left(FieldError(fieldName = name, fieldValue = s, fieldType = fieldType, message = e.toString))
        }
    }
  }

}