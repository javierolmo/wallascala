package com.javi.personal.wallascala.utils.writers

case class DatabaseConnection(host: String, port: Int, user: String, pass: String)

object DatabaseConnection {

  def fromEnv(): DatabaseConnection = {
    val host = readEnvironmentVariable("DB_HOST")
    val port = readEnvironmentVariable("DB_PORT").toInt
    val user = readEnvironmentVariable("DB_USER")
    val pass = readEnvironmentVariable("DB_PASS")
    new DatabaseConnection(host, port, user, pass)
  }

  private def readEnvironmentVariable(variableName: String): String =
    sys.env.getOrElse(variableName, throw new RuntimeException(s"Environment variable $variableName not found"))
}