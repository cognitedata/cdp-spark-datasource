package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefaultValue,
  PropertyType
}

object FDMContainerPropertyTypes {

  val DateNonListWithDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
  )

  val JsonNonListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
  )

  val DateNonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Date NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Date-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    )

  val Float32NonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float32 NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val BooleanListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Boolean-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
  )

  val TextPropertyNonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val JsonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
  )

  val Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val DirectNodeRelationPropertyNonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test DirectNodeRelationProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-DirectNodeRelationProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = DirectNodeRelationProperty(None),
    )

  val Float64NonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float64 NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val Float64NonListWithDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
    description = Some("Test Float64 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float64-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
  )

  val Float64NonListWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description = Some("Test Float64 NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val JsonNonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Json NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Json-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val Float64NonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float64 NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val JsonListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
  )

  val DateNonListWithDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
  )

  val Float32NonListWithDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
    description = Some("Test Float32 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float32-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
  )

  val Int64ListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
  )

  val TextPropertyListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    )

  val TextPropertyNonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val TimestampListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp List WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-List-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    )

  val JsonNonListWithDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
  )

  val DateListWithoutDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
  )

  val TimestampNonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val DateNonListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
  )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val Float32ListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float32 List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    )

  val DateListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
  )

  val Float32NonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float32 NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val BooleanListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Boolean List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    )

  val TextPropertyNonListWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description = Some("Test TextProperty NonList WithDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val BooleanNonListWithDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Boolean(false)),
    description = Some("Test Boolean NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Boolean-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
  )

  val TimestampNonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val Int32ListWithoutDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
  )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val Int64ListWithoutDefaultValueNonNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
  )

  val JsonNonListWithDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
  )

  val TimestampListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val Float64ListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float64 List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    )

  val TextPropertyNonListWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description = Some("Test TextProperty NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val TimestampNonListWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845+01:00")),
      description = Some("Test Timestamp NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val BooleanNonListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Boolean NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-Boolean-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val BooleanNonListWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Boolean NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val TimestampNonListWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845Z")),
      description = Some("Test Timestamp NonList WithDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val TextPropertyListWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty List WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-List-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    )

  val Int32ListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
  )

  val Float32ListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
  )

  val BooleanNonListWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description = Some("Test Boolean NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val Float64ListWithoutDefaultValueNullable: ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
  )

  val Float32NonListWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description = Some("Test Float32 NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val AllPossibleContainerPropertyDefs: Vector[ContainerPropertyDefinition] = Vector(
    DateNonListWithDefaultValueNonNullable,
    JsonNonListWithoutDefaultValueNullable,
    DateNonListWithoutDefaultValueNonNullable,
    Float32NonListWithoutDefaultValueNonNullable,
    BooleanListWithoutDefaultValueNullable,
    TextPropertyNonListWithoutDefaultValueNullable,
    JsonListWithoutDefaultValueNonNullable,
    Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable,
    Int32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    DirectNodeRelationPropertyNonListWithoutDefaultValueNullable,
    Float64NonListWithoutDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithDefaultValueNullable,
    Float64NonListWithDefaultValueNonNullable,
    JsonNonListWithoutDefaultValueNonNullable,
    Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
    Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Float64NonListWithoutDefaultValueNonNullable,
    JsonListWithoutDefaultValueNullable,
    DateNonListWithDefaultValueNullable,
    Float32NonListWithDefaultValueNullable,
    Int64ListWithoutDefaultValueNullable,
    TextPropertyListWithoutDefaultValueNonNullable,
    TextPropertyNonListWithoutDefaultValueNonNullable,
    TimestampListWithoutDefaultValueNullable,
    JsonNonListWithDefaultValueNonNullable,
    DateListWithoutDefaultValueNonNullable,
    TimestampNonListWithoutDefaultValueNonNullable,
    DateNonListWithoutDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Float32ListWithoutDefaultValueNonNullable,
    DateListWithoutDefaultValueNullable,
    Float32NonListWithoutDefaultValueNullable,
    BooleanListWithoutDefaultValueNonNullable,
    TextPropertyNonListWithDefaultValueNullable,
    BooleanNonListWithDefaultValueNullable,
    TimestampNonListWithoutDefaultValueNullable,
    Int32ListWithoutDefaultValueNonNullable,
    Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int64ListWithoutDefaultValueNonNullable,
    JsonNonListWithDefaultValueNullable,
    TimestampListWithoutDefaultValueNonNullable,
    Int32NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Float64ListWithoutDefaultValueNonNullable,
    TextPropertyNonListWithDefaultValueNonNullable,
    Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable,
    TimestampNonListWithDefaultValueNonNullable,
    BooleanNonListWithoutDefaultValueNullable,
    BooleanNonListWithoutDefaultValueNonNullable,
    TimestampNonListWithDefaultValueNullable,
    TextPropertyListWithoutDefaultValueNullable,
    Int32ListWithoutDefaultValueNullable,
    Float32ListWithoutDefaultValueNullable,
    BooleanNonListWithDefaultValueNonNullable,
    Float64ListWithoutDefaultValueNullable,
    Float32NonListWithDefaultValueNonNullable
  )
}
