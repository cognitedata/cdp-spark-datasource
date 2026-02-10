package cognite.spark.v1.fdm.utils

import cognite.spark.v1.{DefaultSource, SparkTest}
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import com.cognite.sdk.scala.v1.fdm.instances.InstanceType
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row}
import org.scalatest.Matchers

object FDMSparkDataframeTestOperations extends SparkTest with Matchers {


  def insertRowsToModel(
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

  def insertNodeRows(
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
      .option("collectMetrics", value = true)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .save()

  def insertEdgeRows(
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

  def readRows(
    instanceType: InstanceType,
    viewSpaceExternalId: String,
    viewExternalId: String,
    viewVersion: String,
    instanceSpaceExternalId: String,
    useQuery: Boolean = false): DataFrame = {
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
      .option("useQuery", useQuery)
      .load()
  }



  def readRows(edgeSpace: String, edgeExternalId: String, useQuery: Boolean = false): DataFrame =
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
        .option("instanceType", "edge")
        .option("metricsPrefix", s"$edgeExternalId-$viewVersion")
        .option("collectMetrics", value = true)
        .load()


  def readRowsFromModel(
      modelSpace: String,
      modelExternalId: String,
      modelVersion: String,
      viewExternalId: String,
      instanceSpace: Option[String],
      debug: Boolean = false,
      useQuery: Boolean = false): DataFrame = {
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
        .option("collectMetrics", value = true)
        .option("sendDebugFlag", value = debug)
        .option("useQuery", value = useQuery)
        .load()
  }

  def readRowsFromModel(
     modelSpace: String,
     modelExternalId: String,
     modelVersion: String,
     edgeTypeSpace: String,
     edgeTypeExternalId: String,
     useQuery: Boolean = false
  ): DataFrame =
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
      .option("useQuery", useQuery)
      .load()

  def syncRows(
    instanceType: InstanceType,
    viewSpaceExternalId: String,
    viewExternalId: String,
    viewVersion: String,
    cursor: String
  ): DataFrame =
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
      .option("cursor", cursor)
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", value = true)
      .load()

  def toExternalIds(rows: Array[Row]): Seq[String] =
    rows.toIndexedSeq.map(row => row.getString(row.schema.fieldIndex("externalId")))

  def toPropVal(rows: Array[Row], prop: String): Seq[String] =
    rows.toIndexedSeq.map(row => row.getString(row.schema.fieldIndex(prop)))

}
