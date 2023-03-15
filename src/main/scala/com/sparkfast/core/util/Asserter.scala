package com.sparkfast.core.util

import com.sparkfast.core.logger.LoggerMixin
import org.slf4j.Logger


object Asserter extends LoggerMixin {

  private class FailedAssertionError(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  def assert(assertion: Boolean, falseMsg: Any = null, logger: Logger = null): Unit = {
    if (!assertion) {
      val msg = "Assertion failed" + (if (falseMsg != null) ": " + falseMsg else "")
      if (logger != null) logger.error(msg) else log.error(msg)
      throw new FailedAssertionError(msg)
    }
  }
}
