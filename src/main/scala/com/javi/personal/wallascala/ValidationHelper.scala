package com.javi.personal.wallascala

/**
 * Provides validation utilities for configuration and input parameters.
 * Helps ensure data integrity and fail-fast behavior.
 */
object ValidationHelper {

  /**
   * Validates that a string is not null or empty.
   * @param value the string to validate
   * @param paramName the parameter name for error messages
   * @throws WallaScalaException if validation fails
   */
  def requireNonEmpty(value: String, paramName: String): Unit = {
    if (value == null || value.trim.isEmpty) {
      throw WallaScalaException(s"Parameter '$paramName' cannot be null or empty")
    }
  }

  /**
   * Validates that an optional value is present.
   * @param value the optional value to validate
   * @param paramName the parameter name for error messages
   * @throws WallaScalaException if validation fails
   * @return the unwrapped value
   */
  def requirePresent[T](value: Option[T], paramName: String): T = {
    value.getOrElse(throw WallaScalaException(s"Parameter '$paramName' is required but not provided"))
  }

  /**
   * Validates that a numeric value is positive.
   * @param value the number to validate
   * @param paramName the parameter name for error messages
   * @throws WallaScalaException if validation fails
   */
  def requirePositive(value: Int, paramName: String): Unit = {
    if (value <= 0) {
      throw WallaScalaException(s"Parameter '$paramName' must be positive, but was $value")
    }
  }

  /**
   * Validates a value against a predicate.
   * @param value the value to validate
   * @param predicate the validation predicate
   * @param errorMessage the error message if validation fails
   * @throws WallaScalaException if validation fails
   */
  def require[T](value: T, predicate: T => Boolean, errorMessage: => String): Unit = {
    if (!predicate(value)) {
      throw WallaScalaException(errorMessage)
    }
  }
}
