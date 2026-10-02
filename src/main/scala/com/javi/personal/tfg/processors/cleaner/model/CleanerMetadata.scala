package com.javi.personal.tfg.processors.cleaner.model

import com.javi.personal.tfg.processors.cleaner.FieldCleaner

case class CleanerMetadata(id: String, fields: Seq[FieldCleaner])
