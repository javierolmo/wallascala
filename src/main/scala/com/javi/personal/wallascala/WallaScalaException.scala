package com.javi.personal.wallascala

case class WallaScalaException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)
