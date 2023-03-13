package com.sparkfast.core.util

object OsUtil {
  def getOSEnv(name: String): Option[String] = sys.env.get(name)

  def getOSEnvFallback(names: String*): Option[String] = names
    .map(n => sys.env.get(n)) collectFirst { case Some(a) => a }

  def isOSEnvExists(name: String): Boolean = getOSEnv(name).isDefined

  def isOSEnvFallbackExists(names: String*): Boolean = getOSEnvFallback(names: _*).isDefined
}
