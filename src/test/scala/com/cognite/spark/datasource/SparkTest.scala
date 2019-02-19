package com.cognite.spark.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")

trait SparkTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()
}
