package cognite.spark.v1.wdl

final case class Datum(
    /* Amount of a given unit. */
    value: Double,
    unit: DistanceUnitEnum.DistanceUnit,
    /* The name of the reference point. Eg. \"KB\" for kelly bushing. */
    reference: String,
)
