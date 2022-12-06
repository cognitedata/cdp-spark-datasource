package cognite.spark.v1.wdl

object DistanceUnitEnum extends Enumeration {
  type DistanceUnit = Value

  val meter = Value("meter")

  val foot = Value("foot")

  val inch = Value("inch")

  val yard = Value("yard")
}
