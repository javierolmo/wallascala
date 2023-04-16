package com.javi.personal
package wallascala

case class WallaScalaException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)
