package com.javi.personal.wallascala.launcher

import com.javi.personal.wallascala.utils.writers.Writer
import com.javi.personal.wallascala.utils.reader.Reader

case class LauncherConfig(
                         reader: Reader,
                         writer: Writer,
                         select: Option[Seq[String]] = Option.empty
                         )
