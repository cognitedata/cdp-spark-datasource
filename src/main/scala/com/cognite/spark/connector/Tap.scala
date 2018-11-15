package com.cognite.spark.connector

object Tap {
  def tap[A](effect: A => Unit)(x: A): A = {
    effect(x)
    x
  }
}
