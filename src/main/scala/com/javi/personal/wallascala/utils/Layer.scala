package com.javi.personal.wallascala.utils

sealed trait Layer
case object Processed extends Layer
case object Sanited extends Layer
case object SanitedExcluded extends Layer
case object Staging extends Layer