package cognite.spark.v1.wdl

final case class Wellheads(
    x: Double,
    y: Double,
    /* The coordinate reference systems of the wellhead. */
    crs: String,
)
