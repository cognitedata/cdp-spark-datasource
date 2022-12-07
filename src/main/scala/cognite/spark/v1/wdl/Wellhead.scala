package cognite.spark.v1.wdl

final case class Wellhead(
    x: Double,
    y: Double,
    /* The coordinate reference systems of the wellhead. */
    crs: String,
)
