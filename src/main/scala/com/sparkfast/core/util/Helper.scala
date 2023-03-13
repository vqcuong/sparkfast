package com.sparkfast.core.util

object Helper {
  def coalesce[A](values: Option[A]*): Option[A] = {
    values collectFirst { case Some(a) => a}
  }
}
