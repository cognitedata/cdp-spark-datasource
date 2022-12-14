package cognite.spark.v1

import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefaultValue}
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{PrimitiveProperty, TextProperty}
import com.cognite.sdk.scala.v1.fdm.containers.{ContainerConstraint, ContainerReference, IndexDefinition}
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstancePropertyValue}

import java.time.{LocalDate, ZoneId, ZonedDateTime}

// scalastyle:off method.length
object FDMTestData {
  val Space = "test-space-scala-sdk"

  object VehicleContainer {
    val VehicleContainerProperties: Map[String, ContainerPropertyDefinition] = Map(
      "id" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("unique vehicle id"),
        name = Some("vehicle-identifier"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "manufacturer" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle manufacturer"),
        name = Some("vehicle-manufacturer-name"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "model" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle model"),
        name = Some("vehicle-model-name"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "year" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle manufactured year"),
        name = Some("vehicle-manufactured-year"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Int32),
        nullable = Some(false)
      ),
      "displacement" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle engine displacement in CC"),
        name = Some("vehicle-engine-displacement"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Int32),
        nullable = Some(true)
      ),
      "weight" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle weight in Kg"),
        name = Some("vehicle-weight"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Int64),
        nullable = Some(false)
      ),
      "compressionRatio" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("engine compression ratio"),
        name = Some("compressionRatio"),
        `type` = TextProperty(),
        nullable = Some(true)
      ),
      "turbocharger" -> ContainerPropertyDefinition(
        defaultValue = Some(PropertyDefaultValue.Boolean(false)),
        description = Some("turbocharger availability"),
        name = Some("turbocharger"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Boolean),
        nullable = Some(false)
      )
    )

    val VehicleContainerConstraints: Map[String, ContainerConstraint] = Map(
      "uniqueId" -> ContainerConstraint.UniquenessConstraint(Seq("id"))
    )

    val VehicleContainerIndexes: Map[String, IndexDefinition] = Map(
      "manufacturerIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("manufacturer")),
      "modelIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("model"))
    )

    val UpdatedVehicleContainerProperties
      : Map[String, ContainerPropertyDefinition] = VehicleContainerProperties + ("hybrid" -> ContainerPropertyDefinition(
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description = Some("hybrid feature availability for the vehicle"),
      name = Some("hybrid"),
      `type` = PrimitiveProperty(`type` = PrimitivePropType.Boolean),
      nullable = Some(true)
    ))

    def vehicleInstanceData(containerRef: ContainerReference): Seq[EdgeOrNodeData] = Seq(
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("1"),
            "manufacturer" -> InstancePropertyValue.String("Toyota"),
            "model" -> InstancePropertyValue.String("RAV-4"),
            "year" -> InstancePropertyValue.Integer(2020),
            "displacement" -> InstancePropertyValue.Integer(2487),
            "weight" -> InstancePropertyValue.Integer(1200L),
            "compressionRatio" -> InstancePropertyValue.String("13 to 1"),
            "turbocharger" -> InstancePropertyValue.Boolean(true)
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("2"),
            "manufacturer" -> InstancePropertyValue.String("Toyota"),
            "model" -> InstancePropertyValue.String("Prius"),
            "year" -> InstancePropertyValue.Integer(2018),
            "displacement" -> InstancePropertyValue.Integer(2487),
            "weight" -> InstancePropertyValue.Integer(1800L),
            "compressionRatio" -> InstancePropertyValue.String("13 to 1"),
            "turbocharger" -> InstancePropertyValue.Boolean(true)
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("3"),
            "manufacturer" -> InstancePropertyValue.String("Volkswagen"),
            "model" -> InstancePropertyValue.String("ID.4"),
            "year" -> InstancePropertyValue.Integer(2022),
            "weight" -> InstancePropertyValue.Integer(2224),
            "turbocharger" -> InstancePropertyValue.Boolean(false)
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("4"),
            "manufacturer" -> InstancePropertyValue.String("Volvo"),
            "model" -> InstancePropertyValue.String("XC90"),
            "year" -> InstancePropertyValue.Integer(2002),
            "weight" -> InstancePropertyValue.Integer(2020),
            "compressionRatio" -> InstancePropertyValue.String("17 to 1"),
            "displacement" -> InstancePropertyValue.Integer(2401),
            "turbocharger" -> InstancePropertyValue.Boolean(true)
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("5"),
            "manufacturer" -> InstancePropertyValue.String("Volvo"),
            "model" -> InstancePropertyValue.String("XC90"),
            "year" -> InstancePropertyValue.Integer(2002),
            "weight" -> InstancePropertyValue.Integer(2020),
            "compressionRatio" -> InstancePropertyValue.String("17 to 1"),
            "displacement" -> InstancePropertyValue.Integer(2401),
            "turbocharger" -> InstancePropertyValue.Boolean(true)
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "id" -> InstancePropertyValue.String("6"),
            "manufacturer" -> InstancePropertyValue.String("Mitsubishi"),
            "model" -> InstancePropertyValue.String("Outlander"),
            "year" -> InstancePropertyValue.Integer(2021),
            "weight" -> InstancePropertyValue.Integer(1745),
            "compressionRatio" -> InstancePropertyValue.String("17 to 1"),
            "displacement" -> InstancePropertyValue.Integer(2000),
            "turbocharger" -> InstancePropertyValue.Boolean(true)
          )
        )
      )
    )
  }

  object RentalRecordsContainer {
    val RentalRecordsContainerProperties: Map[String, ContainerPropertyDefinition] = Map(
      "itemId" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("rented item id"),
        name = Some("rented-item-id"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "renterId" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("id of the person renting the item"),
        name = Some("renter-id"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "from" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("item rented from timestamp"),
        name = Some("rented-from"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Timestamp),
        nullable = Some(false)
      ),
      "to" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("item rented to timestamp"),
        name = Some("rented-to"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Timestamp),
        nullable = Some(false)
      ),
      "invoiceId" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("invoice id for the rent payment"),
        name = Some("invoice-id"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "rating" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("rating provided by the renter out of 10"),
        name = Some("rating"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Float32),
        nullable = Some(true)
      ),
    )

    val RentableContainerConstraints: Map[String, ContainerConstraint] = Map(
      "uniqueId" -> ContainerConstraint.UniquenessConstraint(Seq("itemId", "renterId", "from"))
    )

    val RentableContainerIndexes: Map[String, IndexDefinition] = Map(
      "renterIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("renterId")),
      "itemIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("itemId"))
    )

    def rentableInstanceData(containerRef: ContainerReference): Seq[EdgeOrNodeData] = Seq(
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "itemId" -> InstancePropertyValue.String("1"),
            "renterId" -> InstancePropertyValue.String("222222"),
            "from" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 1, 1, 9, 0, 0, 0, ZoneId.of("GMT+1"))),
            "to" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 1, 14, 18, 0, 0, 0, ZoneId.of("GMT+1"))),
            "invoiceId" -> InstancePropertyValue.String("inv-1"),
            "rating" -> InstancePropertyValue.Double(5.6),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "itemId" -> InstancePropertyValue.String("2"),
            "renterId" -> InstancePropertyValue.String("222222"),
            "from" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 2, 1, 9, 0, 0, 0, ZoneId.of("GMT+1"))),
            "to" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 2, 14, 18, 0, 0, 0, ZoneId.of("GMT+1"))),
            "invoiceId" -> InstancePropertyValue.String("inv-2"),
            "rating" -> InstancePropertyValue.Double(9.0),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "itemId" -> InstancePropertyValue.String("3"),
            "renterId" -> InstancePropertyValue.String("333333"),
            "from" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 2, 1, 9, 0, 0, 0, ZoneId.of("GMT+1"))),
            "to" -> InstancePropertyValue.Timestamp(
              ZonedDateTime.of(2020, 2, 14, 18, 0, 0, 0, ZoneId.of("GMT+1"))),
            "invoiceId" -> InstancePropertyValue.String("inv-3"),
          )
        )
      )
    )
  }

  object PersonContainer {
    val PersonContainerProperties: Map[String, ContainerPropertyDefinition] = Map(
      "nationalId" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("national identification number"),
        name = Some("nationalId"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "firstname" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("firstname of the person"),
        name = Some("firstname"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "lastname" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("lastname of the person"),
        name = Some("lastname"),
        `type` = TextProperty(),
        nullable = Some(false)
      ),
      "dob" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("vehicle model"),
        name = Some("vehicle-model-name"),
        `type` = PrimitiveProperty(`type` = PrimitivePropType.Date),
        nullable = Some(false)
      ),
      "nationality" -> ContainerPropertyDefinition(
        defaultValue = None,
        description = Some("nationality by birth"),
        name = Some("nationality"),
        `type` = TextProperty(),
        nullable = Some(false)
      )
    )

    val PersonContainerConstraints: Map[String, ContainerConstraint] = Map(
      "nationalIdAndNationality" -> ContainerConstraint.UniquenessConstraint(
        Seq("nationalId", "nationality"))
    )

    val PersonContainerIndexes: Map[String, IndexDefinition] = Map(
      "nationalityIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("nationality")),
      "nationalIdIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("nationalId")),
      "firstnameIndex" -> IndexDefinition.BTreeIndexDefinition(Seq("firstname"))
    )

    def personInstanceData(containerRef: ContainerReference): Seq[EdgeOrNodeData] = Seq(
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("111111"),
            "firstname" -> InstancePropertyValue.String("Sadio"),
            "lastname" -> InstancePropertyValue.String("Mane"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1989, 11, 23)),
            "nationality" -> InstancePropertyValue.String("Norwegian"),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("222222"),
            "firstname" -> InstancePropertyValue.String("Alexander"),
            "lastname" -> InstancePropertyValue.String("Arnold"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1989, 10, 23)),
            "nationality" -> InstancePropertyValue.String("Norwegian"),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("333333"),
            "firstname" -> InstancePropertyValue.String("Harry"),
            "lastname" -> InstancePropertyValue.String("Kane"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1990, 10, 20)),
            "nationality" -> InstancePropertyValue.String("Norwegian"),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("444444"),
            "firstname" -> InstancePropertyValue.String("John"),
            "lastname" -> InstancePropertyValue.String("Gotty"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1978, 9, 20)),
            "nationality" -> InstancePropertyValue.String("Italian"),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("555555"),
            "firstname" -> InstancePropertyValue.String("Angela"),
            "lastname" -> InstancePropertyValue.String("Merkel"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1978, 5, 20)),
            "nationality" -> InstancePropertyValue.String("Norwegian"),
          )
        )
      ),
      EdgeOrNodeData(
        source = containerRef,
        properties = Some(
          Map(
            "nationalId" -> InstancePropertyValue.String("666666"),
            "firstname" -> InstancePropertyValue.String("Elon"),
            "lastname" -> InstancePropertyValue.String("Musk"),
            "dob" -> InstancePropertyValue.Date(LocalDate.of(1982, 5, 20)),
            "nationality" -> InstancePropertyValue.String("American"),
          )
        )
      )
    )
  }
}
// scalastyle:on
