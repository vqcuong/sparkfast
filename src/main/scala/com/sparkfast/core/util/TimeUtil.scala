package com.sparkfast.core.util

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, Instant}

object TimeUtil {
  lazy val isoDateTimeParser: DateTimeFormatter = ISODateTimeFormat.dateTimeParser()
  private val defaultZoneID = "Asia/Ho_Chi_Minh"
  private val defaultDateFormat = "yyyy-MM-dd"
  private val defaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss"
  private val defaultDateFormatter = DateTimeFormat.forPattern(defaultDateFormat)

  def getZone(zoneID: String): DateTimeZone = {
    if (zoneID == "UTC") DateTimeZone.UTC else DateTimeZone.forID(zoneID)
  }

  def toInstant(dateTimeString: String, format: String, zoneId: String = "UTC"): Instant = {
    try {
      DateTimeFormat
        .forPattern(format)
        .withZone(getZone(zoneId))
        .parseDateTime(dateTimeString)
        .toInstant
    } catch {
      case _: Exception => Instant.parse(dateTimeString)
    }
  }

  def toInstant(milliSeconds: Long): Instant = Instant.ofEpochMilli(milliSeconds)

  def toDateTime(milliseconds: Long, format: String, zoneId: String = "UTC"): String = {
    new DateTime(milliseconds, getZone(zoneId)).toString(DateTimeFormat.forPattern(format))
  }

  def getToDate(zoneId: String = "UTC"): String = {
    new DateTime().toString(defaultDateFormatter.withZone(getZone(zoneId)))
  }

  def getDateFromMillis(milliseconds: Long, zoneId: String = "UTC"): String = {
    new DateTime(milliseconds).toString(defaultDateFormatter.withZone(getZone(zoneId)))
  }

  def getDateFromInstant(instant: Instant, zoneId: String = "UTC"): String = {
    instant.toString(defaultDateFormatter.withZone(getZone(zoneId)))
  }

  def getDateFromDateTime(dateTimeString: String, format: String, zoneId: String = "UTC"): String = {
    val zone = getZone(zoneId)
    try {
      val instant = toInstant(dateTimeString, format)
      new DateTime(instant.getMillis, zone).toString(defaultDateFormatter)
    } catch {
      case _: Exception => DateTime.parse(dateTimeString).withZone(zone).toString()
    }
  }

  def getHourMilliseconds(milliSeconds: Long): Long = milliSeconds - milliSeconds % 3600000

  def getHourSeconds(seconds: Long): Long = seconds - seconds % 3600

  def toMillis(dateTimeString: String, format: String, zoneId: String = "UTC"): Long = {
    DateTimeFormat
      .forPattern(format)
      .withZone(getZone(zoneId))
      .parseMillis(dateTimeString)
  }
}
