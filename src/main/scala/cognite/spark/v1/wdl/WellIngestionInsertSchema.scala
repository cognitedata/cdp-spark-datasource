package cognite.spark.v1.wdl

final case class WellIngestionInsertSchema(
    /* Name of the well. */
    name: String,
    /* Connection between this well and the well asset with a given source. */
    source: AssetSource,
    /* Unique identifier used to match wells from different sources. The matchingId must be unique within a source. */
    matchingId: Option[String] = None,
    /* Description of the well. */
    description: Option[String] = None,
    /* Also called UWI. */
    uniqueWellIdentifier: Option[String] = None,
    /* Country of the well. */
    country: Option[String] = None,
    /* The quadrant of the well. This is the first part of the unique well identifier used on the norwegian continental shelf. The well `15/9-19-RS` in the VOLVE field is in quadrant `15`. */
    quadrant: Option[String] = None,
    /* Region of the well. */
    region: Option[String] = None,
    /* The date a new well was spudded or the date of first actual penetration of the earth with a drilling bit. */
    //spudDate: Option[java.time.LocalDate] = None,
    /* The block of the well. This is the second part of the unique well identifer used on the norwegian continental shelf. The wellbore `15/9-19-RS` in the VOLVE fild is in block `15/9`. */
    block: Option[String] = None,
    /* Field of the well. */
    field: Option[String] = None,
    /* Operator that owns the well. */
    operator: Option[String] = None,
    /* Exploration or development. */
    wellType: Option[String] = None,
    /* Well licence. */
    license: Option[String] = None,
    /* Water depth of the well. Vertical distance from the mean sea level (MSL) to the sea bed. */
    waterDepth: Option[Distance] = None,
    wellhead: Option[Wellheads] = None,
)
