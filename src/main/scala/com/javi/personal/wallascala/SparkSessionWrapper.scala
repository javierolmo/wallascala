package com.javi.personal.wallascala

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("com/javi/personal/wallascala")
    .config("fs.azure.account.key.melodiadl.blob.core.windows.net", "hZ2NsPin9BJc/6tvs2VQjIBwYbnNfiIcTddC8lBVpJS/4aTpr65+60oLtpNRBONI0VGHaG+ri2/E+ASt0QFNTg==")
    .getOrCreate

}
