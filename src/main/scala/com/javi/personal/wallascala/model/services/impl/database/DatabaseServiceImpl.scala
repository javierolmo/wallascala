package com.javi.personal.wallascala.model.services.impl.database

import com.javi.personal.wallascala.model.services.DatabaseService
import com.javi.personal.wallascala.model.services.impl.blob.model.WriteConfig
import org.apache.spark.sql.DataFrame

class DatabaseServiceImpl(dataSource: DataSource) extends DatabaseService {

  override def write(dataFrame: DataFrame, tableName: String): Unit = {
    dataFrame.write.format("jdbc")
      .option("url", s"jdbc:sqlserver://${dataSource.host}:${dataSource.port}/databaseName=${dataSource.database}")
      .option("dbtable", tableName)
      .option("user", dataSource.user)
      .option("password", dataSource.password)
      .save()
  }

}
