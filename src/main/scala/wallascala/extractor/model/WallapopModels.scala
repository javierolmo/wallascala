package com.javi.personal
package wallascala.extractor.model

case class WallaItemsResult (
  search_objects: Seq[WallaItem],
  from: Int,
  to: Int,
  distance_ordered: Boolean,
  keywords: String,
  order: String,
  search_point: WallaLocation
)

case class WallaItem(
  id: String,
  title: String,
  description: String,
  distance: Float,
  images: Seq[WallaImage],
  user: WallaUser,
  flags: WallaFlags,
  visibility_flags: WallaVisibilityFlags,
  price: Float,
  free_shipping: Boolean,
  web_slug: String,
  category_id: Int,
  supports_shipping: Boolean,
  shipping_allowed: Boolean,
  seller_id: String,
  favorited: Boolean
)

case class WallaUser(
  id: String,
  micro_name: String,
  image: WallaImage,
  online: Boolean,
  kind: String,
)

case class WallaImage(
  original: String,
  xsmall: String,
  small: String,
  large: String,
  medium: String,
  xlarge: String,
  original_width: Int,
  original_height: Int
)

case class WallaFlags (
  pending: Boolean,
  sold: Boolean,
  reserved: Boolean,
  banned: Boolean,
  expired: Boolean,
  onhold: Boolean
)

case class WallaVisibilityFlags(
  bumped: Boolean,
  highlighted: Boolean,
  urgent: Boolean,
  country_bumped: Boolean,
  boosted: Boolean
)

case class WallaLocation (
  latitude: Double,
  longitude: Double
)





