package com.javi.personal.wallascala.utils

sealed trait Layer {
  val name: String
  override def toString: String = name
}

case object Processed extends Layer {override val name = "processed"}
case object Sanited extends Layer {override val name = "sanited"}
case object SanitedExcluded extends Layer {override val name = "sanited_excluded"}
case object Staging extends Layer {override val name = "staging"}
case object Raw extends Layer {override val name = "raw"}
