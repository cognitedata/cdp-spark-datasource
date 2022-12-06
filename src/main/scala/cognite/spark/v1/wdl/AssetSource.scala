package cognite.spark.v1.wdl

final case class AssetSource(
    /* Asset external ID. */
    assetExternalId: String,
    /* Name of the source this asset external ID belongs to. */
    sourceName: String,
)
