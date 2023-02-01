package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.DirectNodeRelationProperty
import com.cognite.sdk.scala.v1.fdm.common.properties.{
  PrimitivePropType,
  PropertyDefaultValue,
  PropertyType
}

object FDMViewPropertyTypes {

  val DateNonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateNonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Date-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float32-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Boolean-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test TextProperty NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-TextProperty-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int32 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int32-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
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

  val DirectNodeRelationPropertyNonListWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test DirectNodeRelationProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-DirectNodeRelationProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = DirectNodeRelationProperty(None),
      container = None,
      containerPropertyIdentifier = None
    )

  val Float64NonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float64-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
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

  val Float64NonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
    description = Some("Test Float64 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float64-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float64NonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
    description = Some("Test Float64 NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Float64-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Json-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
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

  val Float64NonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float64-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateNonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
    description = Some("Test Float32 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float32-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64ListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test TextProperty List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-TextProperty-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test TextProperty NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-TextProperty-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Timestamp List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Timestamp-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampNonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Timestamp NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Timestamp-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateNonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
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

  val Float32ListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float32-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float32-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Boolean-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
    description = Some("Test TextProperty NonList WithDefaultValue Nullable Description"),
    name = Some("Test-TextProperty-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Boolean(false)),
    description = Some("Test Boolean NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Boolean-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampNonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Timestamp NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Timestamp-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int32ListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
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

  val Int64ListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Timestamp List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Timestamp-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
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

  val Float64ListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float64-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
    description = Some("Test TextProperty NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-TextProperty-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithAutoIncrementWithoutDefaultValueNullable: ViewPropertyDefinition =
    ViewPropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(true),
      defaultValue = None,
      description =
        Some("Test Int64 NonList WithAutoIncrement WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Int64-NonList-WithAutoIncrement-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845609Z[UTC]")),
    description = Some("Test Timestamp NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Timestamp-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Boolean-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithoutDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Boolean-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampNonListWithDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845367Z[UTC]")),
    description = Some("Test Timestamp NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Timestamp-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test TextProperty List WithoutDefaultValue Nullable Description"),
    name = Some("Test-TextProperty-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int32ListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32ListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Boolean(false)),
    description = Some("Test Boolean NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Boolean-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float64ListWithoutDefaultValueNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithDefaultValueNonNullable: ViewPropertyDefinition = ViewPropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
    description = Some("Test Float32 NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Float32-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val AllPossibleViewPropertyDefs: Vector[ViewPropertyDefinition] = Vector(
    DateNonListWithDefaultValueNonNullable,
    JsonNonListWithoutDefaultValueNullable,
    DateNonListWithoutDefaultValueNonNullable,
    Float32NonListWithoutDefaultValueNonNullable,
    BooleanListWithoutDefaultValueNullable,
    TextPropertyNonListWithoutDefaultValueNullable,
    JsonListWithoutDefaultValueNonNullable,
    Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable,
    Int32NonListWithAutoIncrementWithoutDefaultValueNullable,
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
    Int64NonListWithAutoIncrementWithoutDefaultValueNullable,
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
