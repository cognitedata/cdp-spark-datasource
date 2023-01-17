package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefaultValue,
  PropertyType
}

object FDMViewPropertyTypes {

  val Float64ListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float64-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16T22:27:39.035773Z[UTC]")),
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description = Some("Test Float32 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64ListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Int64 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int64-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some(
        "Test TextProperty NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Date List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Date-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Boolean(false)),
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateNonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16")),
      description =
        Some("Test Date NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description = Some("Test Float64 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Json-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonNonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue =
        io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
      description =
        Some("Test Json NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateNonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateNonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16")),
      description = Some("Test Date NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Date-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Date List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Date-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
      description =
        Some("Test Float64 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Json List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Json-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32ListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DirectNodeRelationPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable
    : ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some(
      "Test DirectNodeRelationProperty NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
    name = Some(
      "Test-DirectNodeRelationProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
    `type` = DirectNodeRelationProperty(None),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyNonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32ListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Int32 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int32-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyNonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonNonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue =
        io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
      description = Some("Test Json NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-16T22:27:39.035532Z[UTC]")),
      description =
        Some("Test Timestamp NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description =
        Some("Test Int32 NonList WithoutAutoIncrement WithDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Boolean-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32ListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float32 List WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float32-List-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Boolean List WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-List-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Float64 NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Float64-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float32NonListWithoutAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
      description =
        Some("Test Float32 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Float32-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyNonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test TextProperty NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonNonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json NonList WithoutAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonNonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test Json NonList WithoutAutoIncrement WithoutDefaultValue Nullable Description"),
      name = Some("Test-Json-NonList-WithoutAutoIncrement-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithAutoIncrementWithDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val AllPossibleViewPropertyDefs: Vector[ViewPropertyDefinition] = Vector(
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
