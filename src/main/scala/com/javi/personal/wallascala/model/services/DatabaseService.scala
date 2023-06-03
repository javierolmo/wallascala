package com.javi.personal.wallascala.model.services

import com.javi.personal.wallascala.model.services.impl.database.{DataSource, DatabaseServiceImpl}
import org.apache.spark.sql.DataFrame

object DatabaseService {

    def apply(dataSource: DataSource): DatabaseService = {
      new DatabaseServiceImpl(dataSource)
    }



}

trait DatabaseService {

  def write(dataFrame: DataFrame, tableName: String): Unit

}
