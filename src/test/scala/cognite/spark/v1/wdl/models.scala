package cognite.spark.v1.wdl

case class Source(
    name: String,
    description: Option[String] = None
)

case class SourceItems(
    items: Seq[Source]
)

case class AssetSource(
    assetExternalId: String,
    sourceName: String
)

case class Datum(
    value: Double,
    unit: String,
    reference: String
)

case class Distance(
    value: Double,
    unit: String
)

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

case class Wellbore(
    matchingId: String,
    name: String,
    description: Option[String] = None,
    wellMatchingId: String,
    parentWellboreMatchingId: Option[String] = None,
    uniqueWellboreIdentifier: Option[String] = None,
    sources: Seq[AssetSource],
    totalDrillingDays: Option[Double] = None,
    kickoffMeasuredDepth: Option[Distance] = None
)

case class Wellhead(
    x: Double,
    y: Double,
    crs: String
)

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

case class DeleteSources(
    items: Seq[Source],
    recursive: Option[Boolean] = None
)

case class WellboreIngestion(
    matchingId: Option[String] = None,
    name: String,
    description: Option[String] = None,
    wellAssetExternalId: String,
    parentWellboreAssetExternalId: Option[String] = None,
    uniqueWellboreIdentifier: Option[String] = None,
    source: AssetSource,
    datum: Option[Datum] = None,
    totalDrillingDays: Option[Double] = None,
    kickoffMeasuredDepth: Option[Distance] = None
)
case class WellboreIngestionItems(
    items: Seq[WellboreIngestion]
)

case class WellIngestionItems(
    items: Seq[WellIngestion]
)

case class WellItems(
    items: Seq[Well],
    wellsCount: Option[Int] = None,
    wellboresCount: Option[Int] = None,
    nextCursor: Option[String] = None
)
