package com.sparkfast.core.util

import org.slf4j.Logger

object StringUtil {

  /**
   * Check str is safe or else and return boolean flag
   * @param str string to check whether it is not null and blank (safe) or else
   * @return boolean flag indicate whether str is safe or else
   */
  def isSafeString(str: String): Boolean = str != null && !str.isBlank

  /**
   * Check str is safe or else and return boolean flag or throw exception
   * @param str string to check whether it is not null and blank (safe) or else
   * @param log logger to expose error message when false occurring
   * @param falseMsg message to expose when false occurring
   * @param raiseErr boolean flag to determine should raise exception when false occurring
   * @return boolean flag indicate whether str is safe or else
   */
  def isSafeString(str: String, log: Logger = null, falseMsg: String = null, raiseErr: Boolean = false): Boolean = {
    if (isSafeString(str)) {
      true
    } else {
      if (raiseErr) {
        throw new RuntimeException(
          if (isSafeString(falseMsg)) falseMsg else "String value must be not null and blank")
      } else if (log != null && isSafeString(falseMsg)) {
        log.warn(falseMsg)
      }
      false
    }
  }

  /**
   * Check and return str itself if it is safe or null
   *
   * @param str string to check whether it is not null and blank (safe) or else
   * @return str itself if it is safe or null
   */
  def assertSafeString(str: String): String = if (isSafeString(str)) str else null

  /**
   * Check and return str itself if it is safe or throw exception
   * @param str string to check whether it is not null and blank (safe) or else
   * @param falseMsg message to expose when false occurring
   * @return str itself if it is safe or throw exception
   */
  def assertSafeString(str: String, falseMsg: String = null): String = {
    Asserter.assert(isSafeString(str),
      coalesceSafeString(falseMsg, "String value must be not null and blank"))
    str
  }

  /**
   * @param values list of strings to filter out and find the first value is not null and blank (safe)
   * @return first safe string or null
   */
  def coalesceSafeString(values: String*): String = {
    val result = values
      .filter(isSafeString)
      .map(Option(_)) collectFirst { case Some(a) => a }
    result.get
  }
}
