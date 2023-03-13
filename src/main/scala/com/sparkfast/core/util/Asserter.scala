package com.sparkfast.core.util

import org.slf4j.Logger

object Asserter {

  private class FailedAssertionError(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  def assert(assertion: Boolean, falseMsg: String = null, logger: Logger = null): Unit = {
    if (!assertion) {
      if (falseMsg != null) {
        if (logger != null) logger.error(falseMsg)
        throw new FailedAssertionError(falseMsg)
      } else {
        throw new FailedAssertionError("Got a failed assertion")
      }
    }
  }
}
