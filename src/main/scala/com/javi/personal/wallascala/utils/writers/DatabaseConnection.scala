package com.javi.personal.wallascala.utils.writers

case class DatabaseConnection(host: String, port: Int, user: String, pass: String)

object DatabaseConnection {

  def fromEnv(): DatabaseConnection = DatabaseConnection(
    host = readEnvironmentVariable("DB_HOST"),
    port = readEnvironmentVariable("DB_PORT").toInt,
    user = readEnvironmentVariable("DB_USER"),
    pass = readEnvironmentVariable("DB_PASS")
  )

  private def readEnvironmentVariable(variableName: String): String =
    sys.env.getOrElse(variableName, throw new RuntimeException(s"Environment variable $variableName not found"))
}