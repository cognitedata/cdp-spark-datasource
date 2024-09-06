package cognite.spark.v1.fdm.utils

import cognite.spark.v1.DefaultSource
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory
import com.cognite.sdk.scala.v1.fdm.instances.InstanceType
import org.apache.spark.sql.DataFrame

trait FDMSparkDataframeTestOperations extends FlexibleDataModelTestBase {
  protected def insertRowsToModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      instanceSpace: Option[String],
      df: DataFrame,
      onConflict: String = "upsert",
      ignoreNullFields: Boolean = true,
      connectionPropertyName: Option[String] = None): Unit = {
    df.write
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://$cluster.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://$cluster.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("viewExternalId", viewExternalId)
      .options(connectionPropertyName.map("connectionPropertyName" -> _).toMap)
      .option("instanceSpace", instanceSpace.orNull)
      .option("onconflict", onConflict)
      .option("collectMetrics", value = true)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("ignoreNullFields", ignoreNullFields)
      .save()
  }

  protected def insertNodeRows(
      instanceType: InstanceType,
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String,
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpace", instanceSpaceExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .save()

  protected def insertEdgeRows(
      edgeTypeSpace: String,
      edgeTypeExternalId: String,
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://$cluster.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://$cluster.cognitedata.com/.default")
      .option("edgeTypeSpace", edgeTypeSpace)
      .option("edgeTypeExternalId", edgeTypeExternalId)
      .option("onconflict", onConflict)
      .option("collectMetrics", value = true)
      .option("metricsPrefix", s"$edgeTypeSpace-$edgeTypeExternalId")
      .save()


  protected def readRows(
      instanceType: InstanceType,
      viewSpaceExternalId: String,
      viewExternalId: String,
      viewVersion: String,
      instanceSpaceExternalId: String): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("instanceSpace", instanceSpaceExternalId)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", true)
      .load()

  protected def readRows(edgeSpace: String, edgeExternalId: String): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://$cluster.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://$cluster.cognitedata.com/.default")
      .option("edgeTypeSpace", edgeSpace)
      .option("edgeTypeExternalId", edgeExternalId)
      .option("metricsPrefix", s"$edgeExternalId-$viewVersion")
      .option("collectMetrics", value = true)
      .load()

  protected def readRowsFromModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      instanceSpace: Option[String]): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://${cluster}.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://${cluster}.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("instanceSpace", instanceSpace.orNull)
      .option("viewExternalId", viewExternalId)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("collectMetrics", true)
      .load()

  protected def readRowsFromModel(
     modelSpace: String,
     modelExternalId: String,
     modelVersion: String,
     edgeTypeSpace: String,
     edgeTypeExternalId: String): DataFrame =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", FlexibleDataModelRelationFactory.ResourceType)
      .option("baseUrl", s"https://$cluster.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("audience", audience)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", project)
      .option("scopes", s"https://$cluster.cognitedata.com/.default")
      .option("modelSpace", modelSpace)
      .option("modelExternalId", modelExternalId)
      .option("modelVersion", modelVersion)
      .option("edgeTypeSpace", edgeTypeSpace)
      .option("edgeTypeExternalId", edgeTypeExternalId)
      .option("metricsPrefix", s"$modelExternalId-$modelVersion")
      .option("collectMetrics", value = true)
      .load()

  protected def getUpsertedMetricsCount(edgeTypeSpace: String, edgeTypeExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$edgeTypeSpace-$edgeTypeExternalId",
      FlexibleDataModelRelationFactory.ResourceType)


  protected def getUpsertedMetricsCountForModel(modelSpace: String, modelExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$modelSpace-$modelExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

}