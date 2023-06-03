package com.javi.personal.wallascala.model.services

import com.javi.personal.wallascala.model.services.impl.secrets.SecretServiceImpl

object SecretService {

  def apply(): SecretService = new SecretServiceImpl()

}

trait SecretService {

  /**
   * Returns the secret value for the given secret name.
   *
   * @param secretName The name of the secret to get.
   * @return The secret value.
   */
  def getSecret(secretName: String): String

}
