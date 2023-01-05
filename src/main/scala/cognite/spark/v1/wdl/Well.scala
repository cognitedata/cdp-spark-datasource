package cognite.spark.v1.wdl

case class Well(
    matchingId: String,
    name: String,
    description: Option[String] = None,
    uniqueWellIdentifier: Option[String] = None,
    country: Option[String] = None,
    quadrant: Option[String] = None,
    region: Option[String] = None,
    block: Option[String] = None,
    field: Option[String] = None,
    operator: Option[String] = None,
    spudDate: Option[String] = None,
    wellType: Option[String] = None,
    license: Option[String] = None,
    wellhead: Wellhead,
    waterDepth: Option[Distance] = None,
    sources: Seq[AssetSource],
    wellbores: Option[Seq[Wellbore]] = None
)
