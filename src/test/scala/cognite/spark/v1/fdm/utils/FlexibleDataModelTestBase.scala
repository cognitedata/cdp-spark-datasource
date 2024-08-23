package cognite.spark.v1.fdm.utils

import cats.effect.IO
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory
import cognite.spark.v1.{DefaultSource, SparkTest}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{ListablePropertyType, PrimitivePropType, PropertyType}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views._
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.{DataFrame, Row}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.UUID
import scala.util.Random

trait FlexibleDataModelTestBase extends SparkTest {

  protected val clientId: String = sys.env("TEST_CLIENT_ID")
  protected val clientSecret: String = sys.env("TEST_CLIENT_SECRET")
  protected val cluster: String = sys.env("TEST_CLUSTER")
  protected val project: String = sys.env("TEST_PROJECT")
  protected val tokenUri: String = sys.env
    .get("TEST_TOKEN_URL")
    .orElse(
      sys.env
        .get("TEST_AAD_TENANT")
        .map(tenant => s"https://login.microsoftonline.com/$tenant/oauth2/v2.0/token"))
    .getOrElse("https://sometokenurl")
  protected val audience = s"https://${cluster}.cognitedata.com"
  protected val client: GenericClient[IO] = getTestClient()

  protected val spaceExternalId = "testSpaceForSparkDatasource"

  protected val viewVersion = "v1"

  protected def syncRows(
                        instanceType: InstanceType,
                        viewSpaceExternalId: String,
                        viewExternalId: String,
                        viewVersion: String,
                        cursor: String): DataFrame =
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
      .option("cursor", cursor)
      .option("instanceType", instanceType.productPrefix)
      .option("viewSpace", viewSpaceExternalId)
      .option("viewExternalId", viewExternalId)
      .option("viewVersion", viewVersion)
      .option("metricsPrefix", s"$viewExternalId-$viewVersion")
      .option("collectMetrics", value = true)
      .load()
  protected def apiCompatibleRandomString(): String =
    UUID.randomUUID().toString.replaceAll("[_\\-x0]", "").substring(0, 5)

  protected def generateNodeExternalId: String = s"randomId${apiCompatibleRandomString()}"

  protected def getUpsertedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsUpserted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def getReadMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsRead(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def getDeletedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsDeleted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  protected def createInstancePropertyValue(
      propName: String,
      propType: PropertyType,
      directNodeReference: DirectRelationReference
  ): Option[InstancePropertyValue] =
    propType match {
      case d: DirectNodeRelationProperty =>
        val ref = d.container.map(_ => directNodeReference)
        Some(InstancePropertyValue.ViewDirectNodeRelation(value = ref))
      case p: ListablePropertyType if p.isList =>
        Some(listContainerPropToInstanceProperty(propName, p))
      case p =>
        Some(nonListContainerPropToInstanceProperty(propName, p))
    }

  protected def listContainerPropToInstanceProperty(
      propName: String,
      propertyType: PropertyType
  ): InstancePropertyValue =
    propertyType match {
      case PropertyType.TextProperty(Some(true), _) =>
        InstancePropertyValue.StringList(List(s"${propName}Value1", s"${propName}Value2"))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)) =>
        InstancePropertyValue.BooleanList(List(true, false, true, false))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)) =>
        InstancePropertyValue.Int32List((1 to 10).map(_ => Random.nextInt(10000)).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)) =>
        InstancePropertyValue.Int64List((1 to 10).map(_ => Random.nextLong()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)) =>
        InstancePropertyValue.Float32List((1 to 10).map(_ => Random.nextFloat()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)) =>
        InstancePropertyValue.Float64List((1 to 10).map(_ => Random.nextDouble()).toList)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)) =>
        InstancePropertyValue.DateList(
          (1 to 10).toList.map(i => LocalDate.now().minusDays(i.toLong))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)) =>
        InstancePropertyValue.TimestampList(
          (1 to 10).toList.map(i => LocalDateTime.now().minusDays(i.toLong).atZone(ZoneId.of("UTC")))
        )
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)) =>
        InstancePropertyValue.ObjectList(
          List(
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("a"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(true)
                )
              )
            ),
            Json.fromJsonObject(
              JsonObject.fromMap(
                Map(
                  "a" -> Json.fromString("b"),
                  "b" -> Json.fromInt(1),
                  "c" -> Json.fromBoolean(false),
                  "d" -> Json.fromDoubleOrString(1.56)
                )
              )
            )
          )
        )
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }

  protected def nonListContainerPropToInstanceProperty(
      propName: String,
      propertyType: PropertyType
  ): InstancePropertyValue =
    propertyType match {
      case PropertyType.TextProperty(None | Some(false), _) =>
        InstancePropertyValue.String(s"${propName}Value")
      case PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, _) =>
        InstancePropertyValue.Boolean(false)
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int32, None | Some(false)) =>
        InstancePropertyValue.Int32(Random.nextInt(10000))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Int64, None | Some(false)) =>
        InstancePropertyValue.Int64(Random.nextLong())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float32, None | Some(false)) =>
        InstancePropertyValue.Float32(Random.nextFloat())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Float64, None | Some(false)) =>
        InstancePropertyValue.Float64(Random.nextDouble())
      case PropertyType.PrimitiveProperty(PrimitivePropType.Date, None | Some(false)) =>
        InstancePropertyValue.Date(LocalDate.now().minusDays(Random.nextInt(30).toLong))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, None | Some(false)) =>
        InstancePropertyValue.Timestamp(
          LocalDateTime.now().minusDays(Random.nextInt(30).toLong).atZone(ZoneId.of("UTC")))
      case PropertyType.PrimitiveProperty(PrimitivePropType.Json, None | Some(false)) =>
        InstancePropertyValue.Object(
          Json.fromJsonObject(
            JsonObject.fromMap(
              Map(
                "a" -> Json.fromString("a"),
                "b" -> Json.fromInt(1),
                "c" -> Json.fromBoolean(true)
              )
            )
          )
        )
      case _: PropertyType.DirectNodeRelationProperty =>
        InstancePropertyValue.ViewDirectNodeRelation(None)
      case _: PropertyType.TimeSeriesReference =>
        InstancePropertyValue.TimeSeriesReference("timeseriesExtId1")
      case _: PropertyType.FileReference => InstancePropertyValue.FileReference("fileExtId1")
      case _: PropertyType.SequenceReference => InstancePropertyValue.SequenceReference("sequenceExtId1")
      case other => throw new IllegalArgumentException(s"Unknown value :${other.toString}")
    }

  def toExternalIds(rows: Array[Row]): Array[String] =
    rows.map(row => row.getString(row.schema.fieldIndex("externalId")))

  def toPropVal(rows: Array[Row], prop: String): Array[String] =
    rows.map(row => row.getString(row.schema.fieldIndex(prop)))

}
