package cognite.spark.v1.wdl

case class Source(
    name: String,
    description: Option[String] = None
)

case class SourceItems(
    items: Seq[Source]
)

/// Dummy class that everything can deserialize to if we don't care about the
/// output, or if we want serialize `{}`.
case class EmptyObj()

case class WellboreMergeRules(
    name: Seq[String],
    description: Seq[String],
    datum: Seq[String],
    parents: Seq[String],
    wellTops: Seq[String],
    holeSections: Seq[String],
    trajectories: Seq[String],
    casings: Seq[String],
    totalDrillingDays: Seq[String],
    kickoffMeasuredDepth: Seq[String],
)
object WellboreMergeRules {
  def apply(source: Seq[String]): WellboreMergeRules =
    new WellboreMergeRules(
      source,
      source,
      source,
      source,
      source,
      source,
      source,
      source,
      source,
      source
    )
}

case class WellMergeRules(
    name: Seq[String],
    description: Seq[String],
    country: Seq[String],
    quadrant: Seq[String],
    region: Seq[String],
    block: Seq[String],
    field: Seq[String],
    operator: Seq[String],
    spudDate: Seq[String],
    license: Seq[String],
    wellType: Seq[String],
    waterDepth: Seq[String],
    wellhead: Seq[String],
)
object WellMergeRules {
  def apply(sources: Seq[String]): WellMergeRules =
    new WellMergeRules(
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources,
      sources)
}

case class DeleteSources(
    items: Seq[Source],
)

case class AssetSource(
    assetExternalId: String,
    sourceName: String
)

case class DeleteWells(items: Seq[AssetSource], recursive: Boolean = false)

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
    wellhead: Wellhead,
    sources: Seq[AssetSource],
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
    waterDepth: Option[Distance] = None,
    wellbores: Option[Seq[Wellbore]] = None
)

case class Wellbore(
    matchingId: String,
    name: String,
    wellMatchingId: String,
    sources: Seq[AssetSource],
    description: Option[String] = None,
    parentWellboreMatchingId: Option[String] = None,
    uniqueWellboreIdentifier: Option[String] = None,
    datum: Option[Datum] = None,
    totalDrillingDays: Option[Double] = None,
    kickoffMeasuredDepth: Option[Distance] = None
)

case class WellboreItems(items: Seq[Wellbore])

case class Wellhead(
    x: Double,
    y: Double,
    crs: String
)

case class WellIngestion(
    name: String,
    source: AssetSource,
    matchingId: Option[String] = None,
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
)

case class WellboreIngestion(
    name: String,
    wellAssetExternalId: String,
    source: AssetSource,
    matchingId: Option[String] = None,
    description: Option[String] = None,
    parentWellboreAssetExternalId: Option[String] = None,
    uniqueWellboreIdentifier: Option[String] = None,
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
