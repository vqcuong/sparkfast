package com.sparkfast.core.pool

import scala.collection.mutable

trait SimpleKeyValueCachePool[K, V] {
  private val reserved = new mutable.HashMap[K, V]()

  def checkIn(key: K, value: V): Unit = {
    reserved.put(key, value)
  }

  def checkOut(key: K): Option[V] = {
    reserved.get(key)
  }
}
