package cognite.spark.v1.wdl

final case class Distance(
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
)
