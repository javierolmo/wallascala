package com.javi.personal
package wallascala

import org.apache.spark.sql.functions.col


object Reader {

  def main(args: Array[String]): Unit = {
    Application.spark.read
      .parquet("data/wallapop/raw/planta/year=2022/month=7")
//      .select("price", "distance", "title", "description")
//      .where("sold == true or reserved == true")
//      .where("price > 50")
//      .orderBy( "price", "distance")
      .show(1000, truncate = false)
  }



}
