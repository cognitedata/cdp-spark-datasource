package com.cognite.spark.datasource

object Tap {
  def tap[A](effect: A => Unit)(x: A): A = {
    effect(x)
    x
  }
}
