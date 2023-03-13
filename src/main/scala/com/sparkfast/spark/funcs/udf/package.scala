package com.sparkfast.spark.funcs

package object udf {
  val unixTimeHourFloor: Long => Long = (unixTime: Long) => unixTime - unixTime%3600
}
