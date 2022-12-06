package cognite.spark.v1.wdl

final case class Wellbore(
    /* Unique identifier used to match wellbores from different sources. The matchingId must be unique within a source. */
    matchingId: String,
    /* Name of the wellbore. */
    name: String,
    /* Matching id of the associated well. */
    wellMatchingId: String,
    /* List of sources that are associated to this wellbore. */
    sources: Seq[AssetSource],
    /* Description of the wellbore. */
    description: Option[String] = None,
    /* Parent wellbore if it exists. */
    parentWellboreMatchingId: Option[String] = None,
    /* Also called UBI. */
    uniqueWellboreIdentifier: Option[String] = None,
    datum: Option[Datum] = None,
    /* Total days of drilling for this wellbore */
    totalDrillingDays: Option[Double] = None,
)
