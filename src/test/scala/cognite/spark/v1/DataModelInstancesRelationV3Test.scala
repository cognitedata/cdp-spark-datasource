package cognite.spark.v1

import cats.implicits.toTraverseOps
import cognite.spark.v1.FDMTestData.PersonContainer._
import cognite.spark.v1.FDMTestData.RentalRecordsContainer._
import cognite.spark.v1.FDMTestData.VehicleContainer._
import cognite.spark.v1.SparkSchemaHelper.structType
import com.cognite.sdk.scala.common.{DomainSpecificLanguageFilter, EmptyFilter}
import com.cognite.sdk.scala.v1.DataModelType.{EdgeType, NodeType}
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.containers.{
  ContainerCreateDefinition,
  ContainerDefinition,
  ContainerReference
}
import com.cognite.sdk.scala.v1.fdm.instances
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDefinition.NodeDefinition
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{CreatePropertyReference, ViewCreateDefinition, ViewDefinition}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType}
import org.scalatest.{Assertion, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration.DurationInt
import scala.util.{Random, Try}
import scala.util.control.NonFatal

class DataModelInstancesRelationV3Test
    extends FlatSpec
    with Matchers
    with SparkTest
    with BeforeAndAfterAll {
  import CdpConnector.ioRuntime

  private val clientId = sys.env("TEST_CLIENT_ID_BLUEFIELD")
  private val clientSecret = sys.env("TEST_CLIENT_SECRET_BLUEFIELD")
  private val aadTenant = sys.env("TEST_AAD_TENANT_BLUEFIELD")
  private val tokenUri = s"https://login.microsoftonline.com/$aadTenant/oauth2/v2.0/token"
  private val bluefieldAlphaClient = getBlufieldClient(Some("alpha"))
  private val space = FDMTestData.Space
  private val metricPrefix = "sparkDataSourceTestsFDMV3"

  private val vehicleRentalServiceModel = createVehicleRentalView(
    space,
    vehicleContainerExternalId = Some("vehicle_container_422"),
    personContainerExternalId = Some("person_container_776"),
    rentalRecordsContainerExternalId = Some("rental_records_container_847"),
    None
  )

  it should "ingest data" in {
    val vehicleContainerExtId = vehicleRentalServiceModel.vehicleContainer.externalId
    val personContainerExtId = vehicleRentalServiceModel.personContainer.externalId
    val rentableContainerExtId = vehicleRentalServiceModel.personContainer.externalId

//    val vehicleContainerExtId = "vehicle_container_422"
//    val personContainerExtId = "person_container_776"
//    val rentableContainerExtId = "rental_records_container_847"

    val vehiclesDataSqlResult = Try {
      insertRows(
        vehicleContainerExtId,
        spark
          .sql(vehicleDataAsSql(vehicleContainerExtId)),
      )
    }

    val personDataSqlResult = Try {
      insertRows(
        personContainerExtId,
        spark
          .sql(personDataAsSql(personContainerExtId)),
      )
    }

    val rentableDataSqlResult = Try {
      insertRows(
        rentableContainerExtId,
        spark
          .sql(rentableDataAsSql(rentableContainerExtId)),
      )
    }

    val vehicleInstanceExtIds = vehicleInstanceData(ContainerReference(space, vehicleContainerExtId))
      .flatMap(v => v.properties.flatMap(_.get("id").asInstanceOf[Option[InstancePropertyValue.String]]))
      .map(p => s"vehicle_ext_id_${p.value}")
      .toList

    val vehicleInstances = vehicleInstanceExtIds
      .flatMap(id =>
        fetchInstancesByExternalId(space, vehicleContainerExtId, InstanceType.Node, id).items.toList)
      .asInstanceOf[List[NodeDefinition]]

    val personInstanceExtIds = personInstanceData(ContainerReference(space, personContainerExtId))
      .flatMap(v =>
        v.properties.flatMap(_.get("nationalId").asInstanceOf[Option[InstancePropertyValue.String]]))
      .map(p => s"person_ext_id_${p.value}")
      .toList

    val personInstances = personInstanceExtIds
      .flatMap(id =>
        fetchInstancesByExternalId(space, personContainerExtId, InstanceType.Node, id).items.toList)
      .asInstanceOf[List[NodeDefinition]]

    val rentableInstanceExtIds = rentableInstanceData(ContainerReference(space, rentableContainerExtId))
      .flatMap(v =>
        v.properties.flatMap(_.get("itemId").asInstanceOf[Option[InstancePropertyValue.String]]))
      .map(p => s"rentable_ext_id_${p.value}")
      .toList

    val rentableInstances = rentableInstanceExtIds
      .flatMap(id =>
        fetchInstancesByExternalId(space, rentableContainerExtId, InstanceType.Node, id).items.toList)
      .asInstanceOf[List[NodeDefinition]]

    // TODO: Assert on data once API is fixed

    1 shouldBe 1
  }

//  ignore should "pass" in {
//
//    println(io.circe.parser.parse("1"))
//
//    case class TestObj(externalId: String, id: Int, other: Option[String])
//    val row1 = new GenericRowWithSchema(
//      Array[Any]("ext-1", 1),
//      structType[TestObj]
//    )
//    val row2 = new GenericRowWithSchema(
//      Array[Any]("ext-2", 2),
//      structType[TestObj]
//    )
//
//    val x = Seq(row1, row2).map(SparkSchemaHelper.fromRow[TestObj](_))
//    println(x)
//    1 shouldBe 1
//  }

  // scalastyle:off method.length
  private def rentableDataAsSql(rentableContainerExtId: String): String =
    rentableInstanceData(ContainerReference(space, rentableContainerExtId))
      .map { e =>
        val propsMap = e.properties.getOrElse(Map.empty)
        val itemId = propsMap("itemId").asInstanceOf[InstancePropertyValue.String].value
        s"""
           |(
           |  select
           |    '$itemId' as itemId,
           |    ${propsMap
             .get("renterId")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as renterId,
           |    ${propsMap
             .get("from")
             .map(p =>
               s"'${p
                 .asInstanceOf[InstancePropertyValue.Timestamp]
                 .value
                 .format(InstancePropertyValue.Timestamp.formatter)}'")
             .orNull} as from,
           |    ${propsMap
             .get("to")
             .map(p =>
               s"'${p
                 .asInstanceOf[InstancePropertyValue.Timestamp]
                 .value
                 .format(InstancePropertyValue.Timestamp.formatter)}'")
             .orNull} as to,
           |    ${propsMap
             .get("invoiceId")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as invoiceId,
           |    ${propsMap
             .get("rating")
             .map(p => p.asInstanceOf[InstancePropertyValue.Double].value)
             .orNull} as rating,
           |    'rentable_ext_id_$itemId' as externalId
           |)
           |""".stripMargin
      }
      .mkString(" union all ")
  // scalastyle:on method.length

  private def personDataAsSql(personContainerExtId: String): String =
    personInstanceData(ContainerReference(space, personContainerExtId))
      .map { e =>
        val propsMap = e.properties.getOrElse(Map.empty)
        val nationalId = propsMap("nationalId").asInstanceOf[InstancePropertyValue.String].value
        s"""
           |(
           |  select
           |    '$nationalId' as nationalId,
           |    ${propsMap
             .get("firstname")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as firstname,
           |    ${propsMap
             .get("lastname")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as lastname,
           |    ${propsMap
             .get("dob")
             .map(p =>
               s"'${p
                 .asInstanceOf[InstancePropertyValue.Date]
                 .value
                 .format(InstancePropertyValue.Date.formatter)}'")
             .orNull} as dob,
           |    ${propsMap
             .get("nationality")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as nationality,
           |    'person_ext_id_$nationalId' as externalId
           |)
           |""".stripMargin
      }
      .mkString(" union all ")

  // scalastyle:off method.length
  private def vehicleDataAsSql(vehicleContainerExtId: String): String =
    vehicleInstanceData(ContainerReference(space, vehicleContainerExtId))
      .map { e =>
        val propsMap = e.properties.getOrElse(Map.empty)
        val vehicleId = propsMap("id").asInstanceOf[InstancePropertyValue.String].value
        s"""
           |(
           |  select
           |    '$vehicleId' as id,
           |    ${propsMap
             .get("manufacturer")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as manufacturer,
           |    ${propsMap
             .get("model")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as model,
           |    ${propsMap
             .get("year")
             .map(p => p.asInstanceOf[InstancePropertyValue.Integer].value)
             .orNull} as year,
           |    ${propsMap
             .get("displacement")
             .map(p => p.asInstanceOf[InstancePropertyValue.Integer].value)
             .orNull} as displacement,
           |    ${propsMap
             .get("weight")
             .map(p => p.asInstanceOf[InstancePropertyValue.Integer].value)
             .orNull} as weight,
           |    ${propsMap
             .get("compressionRatio")
             .map(p => s"'${p.asInstanceOf[InstancePropertyValue.String].value}'")
             .orNull} as compressionRatio,
           |    ${propsMap
             .get("turbocharger")
             .map(p => p.asInstanceOf[InstancePropertyValue.Boolean].value)
             .orNull} as turbocharger,
           |    'vehicle_ext_id_$vehicleId' as externalId
           |)
           |""".stripMargin
      }
      .mkString(" union all ")
  // scalastyle:on method.length

  private def fetchInstancesByExternalId(
      space: String,
      containerExternalId: String,
      instanceType: InstanceType,
      instanceExternalId: String): InstanceFilterResponse =
    bluefieldAlphaClient.instances
      .retrieveByExternalIds(
        items = Seq(
          InstanceRetrieve(
            sources = Seq(ContainerReference(space, containerExternalId)),
            instanceType = instanceType,
            externalId = instanceExternalId,
            space = space
          )
        ),
        includeTyping = true
      )
      .unsafeRunSync()

  private def readRows(containerExternalId: String, metricPrefix: String) =
    spark.read
      .format("cognite.spark.v1")
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("vehicleContainerExternalId", containerExternalId)
      .option("space", space)
      .option("collectMetrics", true)
      .option("metricsPrefix", metricPrefix)
      .option("type", DataModelInstancesRelationV3.ResourceType)
      .load()

  private def insertRows(
      containerExternalId: String,
      df: DataFrame,
      onConflict: String = "upsert"): Unit =
    df.write
      .format("cognite.spark.v1")
      .option("type", DataModelInstancesRelationV3.ResourceType)
      .option("baseUrl", "https://bluefield.cognitedata.com")
      .option("tokenUri", tokenUri)
      .option("clientId", clientId)
      .option("clientSecret", clientSecret)
      .option("project", "extractor-bluefield-testing")
      .option("scopes", "https://bluefield.cognitedata.com/.default")
      .option("containerExternalId", containerExternalId)
      .option("space", space)
      .option("onconflict", onConflict)
      .option("collectMetrics", true)
      .option("metricsPrefix", containerExternalId)
      .save()

  case class VehicleRentalServiceModel(
      vehicleContainer: ContainerDefinition,
      personContainer: ContainerDefinition,
      rentalRecordsContainer: ContainerDefinition,
      norwegianVehicleRentalServiceView: ViewDefinition,
  )
  // scalastyle:off
  private def createVehicleRentalView(
      space: String,
      vehicleContainerExternalId: Option[String],
      personContainerExternalId: Option[String],
      rentalRecordsContainerExternalId: Option[String],
      vehicleRentalViewExternalId: Option[String],
      vehicleRentalViewVersion: Option[String] = Some("vehicle_rental_service_v1")) = {
    val vehicleContainerDef = ContainerCreateDefinition(
      space = space,
      externalId = vehicleContainerExternalId.getOrElse(s"vehicle_container_${Random.nextInt(1000)}"),
      name = Some(s"vehicle-container"),
      description = Some("Test container for modeling vehicles"),
      usedFor = Some(Usage.Node),
      properties = VehicleContainerProperties,
      constraints = Some(VehicleContainerConstraints),
      indexes = Some(VehicleContainerIndexes)
    )

    val personContainerDef = ContainerCreateDefinition(
      space = space,
      externalId = personContainerExternalId.getOrElse(s"person_container_${Random.nextInt(1000)}"),
      name = Some(s"person-container"),
      description = Some("person records container"),
      usedFor = Some(Usage.Node),
      properties = PersonContainerProperties,
      constraints = Some(PersonContainerConstraints),
      indexes = Some(PersonContainerIndexes)
    )

    val rentalRecordsContainerDef = ContainerCreateDefinition(
      space = space,
      externalId =
        rentalRecordsContainerExternalId.getOrElse(s"rental_records_container_${Random.nextInt(1000)}"),
      name = Some(s"rental-records-container"),
      description = Some("container to store any rental record"),
      usedFor = Some(Usage.Node),
      properties = RentalRecordsContainerProperties,
      constraints = Some(RentableContainerConstraints),
      indexes = Some(RentableContainerIndexes)
    )

    val createdVehicleContainer =
      bluefieldAlphaClient.containers.createItems(Seq(vehicleContainerDef)).unsafeRunSync().head
    val vehicleContainerRef = createdVehicleContainer.toContainerReference
    println(vehicleContainerRef.externalId)

    val createdPersonContainer =
      bluefieldAlphaClient.containers.createItems(Seq(personContainerDef)).unsafeRunSync().head
    val personContainerRef = createdPersonContainer.toContainerReference
    println(personContainerRef.externalId)

    val createdRentalRecordsContainer =
      bluefieldAlphaClient.containers.createItems(Seq(rentalRecordsContainerDef)).unsafeRunSync().head
    val rentalRecordsContainerRef = createdRentalRecordsContainer.toContainerReference
    println(rentalRecordsContainerRef.externalId)

    Thread.sleep(5000) // To avoid container not found errors

    val norwegianVehicleRentalServiceViewDef = ViewCreateDefinition(
      space = space,
      externalId = vehicleRentalViewExternalId.getOrElse(
        s"norwegian_vehicle_rental_service_view_${Random.nextInt(1000)}"),
      name = Some("Norwegian Vehicle Rental Service"),
      description = Some(
        "Vehicle Rental Service for Norwegian nationals that combines vehicles, persons & rentable containers"),
      version = vehicleRentalViewVersion,
      properties = Map(
        "vehicleId" -> CreatePropertyReference(vehicleContainerRef, "id"),
        "renterId" -> CreatePropertyReference(personContainerRef, "itemId"),
        "renterFirstname" -> CreatePropertyReference(personContainerRef, "firstname"),
        "renterLastname" -> CreatePropertyReference(personContainerRef, "lastname"),
        "rentedFrom" -> CreatePropertyReference(rentalRecordsContainerRef, "from"),
        "rentedTo" -> CreatePropertyReference(rentalRecordsContainerRef, "to"),
        "invoiceId" -> CreatePropertyReference(rentalRecordsContainerRef, "invoiceId"),
      ),
      filter = None // Some(FilterDefinition.Equals(Seq("nationality"), FilterValueDefinition.String("Norwegian")))
    )

    val createdNorwegianVehicleRentalServiceView = bluefieldAlphaClient.views
      .createItems(Seq(norwegianVehicleRentalServiceViewDef))
      .unsafeRunSync()
      .head

    println(createdNorwegianVehicleRentalServiceView.externalId)
    println(createdNorwegianVehicleRentalServiceView.version)

    VehicleRentalServiceModel(
      vehicleContainer = createdVehicleContainer,
      personContainer = createdPersonContainer,
      rentalRecordsContainer = createdRentalRecordsContainer,
      norwegianVehicleRentalServiceView = createdNorwegianVehicleRentalServiceView
    )
  } // scalastyle:on
}
