package com.javi.personal.wallascala.utils.writers

case class DatabaseConnection(host: String, port: Int, user: String, pass: String)

object DatabaseConnection {

  def fromEnv(): DatabaseConnection = {
    val envVars = readEnvironmentVariables(Seq("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS"))
    fromEnvironmentVariables(envVars)
  }

  def fromEnvironmentVariables(envVars: Map[String, String]): DatabaseConnection = DatabaseConnection(
    host = envVars("DB_HOST"),
    port = envVars("DB_PORT").toInt,
    user = envVars("DB_USER"),
    pass = envVars("DB_PASS")
  )

  private def readEnvironmentVariables(variableNames: Seq[String]): Map[String, String] =
    variableNames.map { name =>
      name -> sys.env.getOrElse(name, throw new RuntimeException(s"Environment variable $name not found"))
    }.toMap
}