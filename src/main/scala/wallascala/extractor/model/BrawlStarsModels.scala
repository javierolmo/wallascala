package com.javi.personal
package wallascala.extractor.model

case class BrawlStarsResult[A](
  items: Seq[A],
  //paging: Paging
)

case class Brawler(
  id: Int,
  name: String,
  starPowers: Seq[Skill],
  gadgets: Seq[Skill]
)

case class Skill(
  id: Int,
  name: String
)

case class Paging (
  cursors: Cursors
)

case class Cursors(
  id: String
)
