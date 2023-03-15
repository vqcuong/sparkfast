package com.sparkfast.core.pool

import com.sparkfast.core.logger.LoggerMixin
import scala.collection.mutable

trait SimpleObjectPool[T] extends LoggerMixin {
  protected val objectName: String = this.getClass.getSimpleName
  protected val verbose: Boolean = true
  protected val expirationTime: Long = 60000L
  protected val locked = new mutable.HashMap[T, Long]()
  protected val unLocked = new mutable.HashMap[T, Long]()

  protected def create(): T

  protected def validate(o: T): Boolean

  protected def expire(o: T): Unit

  def checkOut(): T = synchronized {
    val now: Long = System.currentTimeMillis()
    if (unLocked.nonEmpty) {
      val e: Iterator[T] = unLocked.keys.iterator
      while (e.hasNext) {
        val t = e.next()
        if ((now - unLocked.getOrElse(t, 0L)) > expirationTime) {
          unLocked.remove(t)
          expire(t)
        } else {
          if (validate(t)) {
            unLocked.remove(t)
            locked.put(t, now)
            if (verbose) log.info(s"Check out existed $objectName")
            return t
          } else {
            unLocked.remove(t)
            expire(t)
            if (verbose) log.info(s"Current $objectName was invalidated, expired it")
          }
        }
      }
    }
    val t = create()
    locked.put(t, now)
    if (verbose) log.info(s"Check out new $objectName")
    t
  }

  def checkIn(t: T): Unit = synchronized {
    locked.remove(t)
    unLocked.put(t, System.currentTimeMillis())
    if (verbose) log.info(s"Check in existed $objectName")
  }
}
