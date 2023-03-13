package com.sparkfast.core.logger

import org.slf4j.{Logger, LoggerFactory}

trait LoggerMixin  {
  lazy val log: Logger = LoggerFactory.getLogger(getClass)
}
