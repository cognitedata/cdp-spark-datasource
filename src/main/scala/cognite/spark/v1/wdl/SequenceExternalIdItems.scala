/**
 * Well data layer
 * # Introduction  This is the reference documentation for the Well data layer. The Well data layer enables users to consume subsurface data from multiple sources based on a unified domain data model. The data extracted and stored in Cognite Data Fusion (CDF) will be ready for consumption based on a domain model regardless of how it is stored in the original source.  ## The data model  The figure below shows the relationships between the different data types in the Well data layer.  <img src=\"/datamodel-datatypes.svg\" alt=\"testing\" style=\"display: block; margin: auto; max-width: 700px\" />  ### Multi-source setups  The Well data layer allows you to connect and query data from different sources. Data from different sources should be ingested separately so that they remain independent. The connection between the sources are done only on the _well_ and _wellbore_ level. If two wells or two wellbores share the same `matchingId`, they are deemed to be the same entity, and will be merged. Queries will then be able to search for data from all sources seemlessly.  Let's consider an example:  <img src=\"/datamodel-overview.svg\" alt=\"testing\" style=\"display: block; margin: auto; max-width: 700px\" />  In the figure above, there are two sources: `EDM` and `Petrel Studio`. Each of these sources have their own independent sets of data that are connected using the `matchingId` property on the wells and wellbores.  The properties on the merged wells and wellbores are determined by _merge rules_. Please see the [well merge rules endpoint](#tag/Wells/operation/postWellsMergerules) and the [wellbore merge rules endpoint](#tag/Wellbores/operation/postWellboresMergerules) for more information on what rules are available.  ### Definitive data types  For some data types, there should ideally be only one _definitive_ item. For example: for a given wellbore, there should be a way to get the _best_ trajectory, the _definitive_ one.  The following data types have a read-only property called `isDefinitive`: - trajectories - casing schematics - well top groups - hole section groups  This `isDefinitive` property is determined based on the `phase` and the _wellbore merge rules_ where the `phase` is the most important. The `phase` can be set to `proposed`, `planned`, or `actual`.  There can only be a single item with phase `actual` for each combination of source and wellbore. For example, there can only be a single trajectory with phase `actual` that is connected to the `EDM` representation of wellbore `15/9-19`.  If there are more than one item with the same phase from the source with highest priority, then the oldest one is selected as the definitive one.  ### Inserting and updating items  In the well data layer, there is no distinction between inserts and updates. All the ingestion endpoints are _upserts_.  ## Examples  ### Postman  Our postman collection can be downloaded [here](/wdl-playground-postman.json). Open Postman, click `Import -> Import From Link`, insert the link and import.  You can read more about how to use Postman [here](https://docs.cognite.com/dev/guides/postman/)  ### Python SDK tutorial  Please also see the [python sdk documentation](https://cognite-wells-sdk.readthedocs-hosted.com/en/latest/wdl.html) that has an example notebook that will walk you through the concepts. 
 *
 * The version of the OpenAPI document: playground
 * Contact: support@cognite.com
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */
package cognite.spark.v1.wdl


case class SequenceExternalIdItems(
  items: Seq[SequenceExternalId],
  /* Ignore sequence external ID's that are not found. */
  ignoreUnknownIds: Option[Boolean] = None
)

