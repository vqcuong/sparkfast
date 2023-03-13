package com.sparkfast.core.config

import com.sparkfast.core.logger.LoggerMixin
import com.typesafe.config.{Config, ConfigFactory, ConfigResolveOptions, ConfigUtil}

import scala.collection.JavaConverters._
import java.io.{File, IOException}

trait TypeSafeConfigImpl extends LoggerMixin {
  protected val defaultConfigResource: String = "src/resources/conf/default.conf"
  private final lazy val defaultConfig: Config = getDefaultConfig
  private var config: Config = defaultConfig

  private def getDefaultConfig: Config = {
    try {
      ConfigFactory
        .parseResources(defaultConfigResource)
        .resolve(ConfigResolveOptions.noSystem())
    } catch {
      case _: Exception =>
        log.debug(
          s"Default configuration resource: $defaultConfigResource does not existed, " +
            s"use default application config instead.")
        ConfigFactory.defaultApplication()
    }
  }

  protected final def applyResourceConfig(resourcePath: String = null): Boolean = {
    if (resourcePath == null) return false
    if (this.getClass.getClassLoader.getResource(resourcePath) == null) {
      log.warn(s"Config resource $resourcePath don't exist")
      return false
    }
    try {
      val overwriteConfig = ConfigFactory.parseResources(resourcePath).resolve()
      config = overwriteConfig.withFallback(config)
      log.info(s"Load config resource $resourcePath successful!")
      true
    } catch {
      case e: Exception => throw new Exception(s"UnexpectedException: ${e.getMessage}")
    }
  }

  protected final def applyFileConfig(filePath: String = null): Boolean = {
    if (filePath == null) return false
    try {
      val file = new File(filePath)
      if (file.exists()) {
        val overwriteConfig = ConfigFactory.parseFile(file).resolve()
        config = overwriteConfig.withFallback(config)
        log.info(s"Load config file $filePath successful!")
        true
      } else {
        log.warn(s"Config file $filePath don't exist")
        true
      }
    } catch {
      case _: java.io.IOException => throw new IOException(s"File $filePath not found")
      case e: Exception => throw new Exception(s"UnexpectedException: ${e.getMessage}")
    }
  }

  final def get(path: String): String = config.getString(path)

  final def getMap(path: String = null): Map[String, String] = {
    def unwrapKey(key: String): String = String.join(".", ConfigUtil.splitPath(key))
    val configMap = if (path != null) config.getConfig(path) else config
    configMap
      .entrySet()
      .asScala
      .map(e => (unwrapKey(e.getKey), e.getValue.unwrapped().toString))
      .toMap
  }
}
