package com.javi.personal.wallascala.model

case class WallaScalaException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)
