package com.javi.personal.wallascala

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class StorageAccountLocation(account: String, container: String, path: String, v2: Boolean = false) {

  def cd(relativePath: String): StorageAccountLocation = this.copy(path = f"$path/$relativePath")

  def cd(localDate: LocalDate): StorageAccountLocation =
    cd(s"year=${localDate.getYear}")
      .cd(s"month=${localDate.getMonthValue}")
      .cd(s"day=${localDate.getDayOfMonth}")

  def wasbsURL: String = s"wasbs://$container@$account.blob.core.windows.net/$path"

  def abfssURL: String = s"abfss://$container@$account.dfs.core.windows.net/$path"

  def url: String = if (v2) abfssURL else wasbsURL

  override def toString: String = url
}
