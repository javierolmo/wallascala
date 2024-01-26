package com.javi.personal.wallascala.utils.writers

case class DatabaseConnection(host: String, port: Int, user: String, pass: String)

object DatabaseConnection {
  def fromEnv(): DatabaseConnection = {
    val host = sys.env("DB_HOST")
    val port = sys.env("DB_PORT").toInt
    val user = sys.env("DB_USER")
    val pass = sys.env("DB_PASS")
    new DatabaseConnection(host, port, user, pass)
  }
}