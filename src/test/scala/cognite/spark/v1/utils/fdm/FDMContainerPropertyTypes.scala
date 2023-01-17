package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefaultValue,
  PropertyType
}

object FDMContainerPropertyTypes {

  val Float64ListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float64-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    )

  val BooleanNonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val TimestampNonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16T22:27:39.035773Z[UTC]")),
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val Float32NonListWithAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description = Some("Test Float32 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val Int64ListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Int64 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int64-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    )

  val TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNonNullable
    : ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some(
        "Test TextProperty NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val Int64ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    )

  val Float32NonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val DateListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Date List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Date-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
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

  val Int32NonListWithAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
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

  val TimestampListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    )

  val TimestampNonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val DateNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    )

  val Float32NonListWithAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val BooleanNonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val DateNonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16")),
      description =
        Some("Test Date NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
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

  val Int32ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
    )

  val Float64NonListWithAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description = Some("Test Float64 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val Float64ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    )

  val Float64NonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
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

  val BooleanNonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val JsonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Json-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    )

  val TextPropertyListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    )

  val JsonNonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue =
        io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
      description =
        Some("Test Json NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    )

  val DateNonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    )

  val Int32NonListWithAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
    )

  val DateNonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16")),
      description = Some("Test Date NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    )

  val BooleanNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    )

  val Float64NonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val DateListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Date-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
    )

  val Float64NonListWithAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
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

  val JsonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Json List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Json-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    )

  val Float32ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    )

  val DirectNodeRelationPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable
    : ContainerPropertyDefinition = ContainerPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some(
      "Test DirectNodeRelationProperty NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
    name = Some(
      "Test-DirectNodeRelationProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
    `type` = DirectNodeRelationProperty(None),
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

  val Int64NonListWithAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val TextPropertyNonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val TimestampListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    )

  val Int32ListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Int32 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int32-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
    )

  val TimestampNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    )

  val Float64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val TextPropertyNonListWithoutAutoIncrementWithDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val Float32NonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val JsonNonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue =
        io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
      description = Some("Test Json NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
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

  val TextPropertyListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    )

  val Float32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val TimestampNonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16T22:27:39.035532Z[UTC]")),
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
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

  val BooleanListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Boolean-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    )

  val Float32ListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float32-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    )

  val BooleanListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    )

  val Float64NonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    )

  val Float32NonListWithoutAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    )

  val TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    )

  val JsonNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    )

  val JsonNonListWithoutAutoIncrementWithoutDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    )

  val Int64NonListWithAutoIncrementWithDefaultValueNullable: ContainerPropertyDefinition =
    ContainerPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
    )

  val AllPossibleViewPropertyDefs: Vector[ContainerPropertyDefinition] = Vector(
    Float64ListWithoutAutoIncrementWithoutDefaultValueNullable,
    BooleanNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    TimestampNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Float32NonListWithAutoIncrementWithDefaultValueNullable,
    Int64ListWithoutAutoIncrementWithoutDefaultValueNullable,
    TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Int64ListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float32NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    DateListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Int32NonListWithAutoIncrementWithDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    TimestampListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    TimestampNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    DateNonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float32NonListWithAutoIncrementWithDefaultValueNonNullable,
    BooleanNonListWithoutAutoIncrementWithDefaultValueNullable,
    DateNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Int32NonListWithoutAutoIncrementWithDefaultValueNullable,
    Int32ListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithAutoIncrementWithDefaultValueNullable,
    Float64ListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    BooleanNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    JsonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    TextPropertyListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    JsonNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    DateNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int32NonListWithAutoIncrementWithDefaultValueNonNullable,
    DateNonListWithoutAutoIncrementWithDefaultValueNullable,
    BooleanNonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithoutAutoIncrementWithDefaultValueNullable,
    DateListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithAutoIncrementWithDefaultValueNonNullable,
    Int64NonListWithoutAutoIncrementWithDefaultValueNullable,
    JsonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Float32ListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    DirectNodeRelationPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Int64NonListWithAutoIncrementWithDefaultValueNonNullable,
    TextPropertyNonListWithoutAutoIncrementWithDefaultValueNullable,
    TimestampListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int32ListWithoutAutoIncrementWithoutDefaultValueNullable,
    TimestampNonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    TextPropertyNonListWithoutAutoIncrementWithDefaultValueNonNullable,
    Float32NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    JsonNonListWithoutAutoIncrementWithDefaultValueNullable,
    Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    TextPropertyListWithoutAutoIncrementWithoutDefaultValueNullable,
    Float32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    TimestampNonListWithoutAutoIncrementWithDefaultValueNullable,
    Int32NonListWithoutAutoIncrementWithDefaultValueNonNullable,
    BooleanListWithoutAutoIncrementWithoutDefaultValueNullable,
    Float32ListWithoutAutoIncrementWithoutDefaultValueNullable,
    BooleanListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    Float64NonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Float32NonListWithoutAutoIncrementWithDefaultValueNullable,
    TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    JsonNonListWithoutAutoIncrementWithoutDefaultValueNonNullable,
    JsonNonListWithoutAutoIncrementWithoutDefaultValueNullable,
    Int64NonListWithAutoIncrementWithDefaultValueNullable
  )
}
