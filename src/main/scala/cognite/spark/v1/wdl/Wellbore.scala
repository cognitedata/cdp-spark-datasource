package cognite.spark.v1.wdl

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
