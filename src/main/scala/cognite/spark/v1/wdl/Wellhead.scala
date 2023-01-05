package cognite.spark.v1.wdl

/**
  * Point in space representing the location of the wellhead.
  */
case class Wellhead(
    x: Double,
    y: Double,
    crs: String
)
