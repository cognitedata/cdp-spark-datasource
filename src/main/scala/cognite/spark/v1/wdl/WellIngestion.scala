package cognite.spark.v1.wdl

case class WellIngestion(
    matchingId: Option[String] = None,
    name: String,
    description: Option[String] = None,
    uniqueWellIdentifier: Option[String] = None,
    country: Option[String] = None,
    quadrant: Option[String] = None,
    region: Option[String] = None,
    spudDate: Option[String] = None,
    block: Option[String] = None,
    field: Option[String] = None,
    operator: Option[String] = None,
    wellType: Option[String] = None,
    license: Option[String] = None,
    waterDepth: Option[Distance] = None,
    wellhead: Option[Wellhead] = None,
    source: AssetSource
)
