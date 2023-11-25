package com.javi.personal.wallascala

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class StorageAccountLocation(account: String, container: String, path: String, v2: Boolean = false) {

  def cd(relativePath: String): StorageAccountLocation = this.copy(path = f"$path/$relativePath")

  def cd(localDate: LocalDate): StorageAccountLocation = {
    val yearString = DateTimeFormatter.ofPattern("yyyy").format(localDate)
    val monthString = DateTimeFormatter.ofPattern("MM").format(localDate)
    val dayString = DateTimeFormatter.ofPattern("dd").format(localDate)
    cd(s"year=$yearString/month=$monthString/day=$dayString")
  }

  def wasbsURL: String = s"wasbs://$container@$account.blob.core.windows.net/$path"

  def abfssURL: String = s"abfss://$container@$account.dfs.core.windows.net/$path"

  def url: String = if (v2) abfssURL else wasbsURL

  override def toString: String = url
}
