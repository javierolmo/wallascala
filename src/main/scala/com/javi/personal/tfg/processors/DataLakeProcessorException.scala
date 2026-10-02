package com.javi.personal.tfg.processors

case class DataLakeProcessorException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)
