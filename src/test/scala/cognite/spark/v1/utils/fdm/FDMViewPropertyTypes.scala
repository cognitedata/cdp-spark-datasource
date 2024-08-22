package cognite.spark.v1.utils.fdm

import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewCorePropertyDefinition
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyType.{DirectNodeRelationProperty, EnumValueMetadata}
import com.cognite.sdk.scala.v1.fdm.common.properties.{PrimitivePropType, PropertyDefaultValue, PropertyType}

object FDMViewPropertyTypes {

  val DateNonListWithDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Date-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float32 NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float32-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Boolean-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int32NonListWithAutoIncrementWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Int32NonListWithAutoIncrementWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val DirectNodeRelationPropertyNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test DirectNodeRelationProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-DirectNodeRelationProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = DirectNodeRelationProperty(None, None),
      container = None,
      containerPropertyIdentifier = None
    )
  val DirectNodeRelationViewPropertyListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description =
        Some("Test DirectNodeRelationProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-DirectNodeRelationProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = DirectNodeRelationProperty(None, None, Some(true))
    )

  val Float64NonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float64-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Float64NonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
    description = Some("Test Float64 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float64-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float64NonListWithDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float64(1.21)),
    description = Some("Test Float64 NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Float64-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json NonList WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Json-NonList-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int32(1)),
      description = Some("Test Int32 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int32-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Float64NonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Float64 NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Float64-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val JsonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Json List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Json-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateNonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17")),
    description = Some("Test Date NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
    description = Some("Test Float32 NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Float32-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64ListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val TextPropertyNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Timestamp List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Timestamp-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val DateNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.Int64(12)),
      description = Some("Test Int64 NonList WithoutAutoIncrement WithDefaultValue Nullable Description"),
      name = Some("Test-Int64-NonList-WithoutAutoIncrement-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Float32ListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float32-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val DateListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Date List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Date-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Date, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float32-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Boolean-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description = Some("Test TextProperty NonList WithDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-NonList-WithDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Boolean(false)),
    description = Some("Test Boolean NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Boolean-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp NonList WithoutDefaultValue Nullable Description"),
      name = Some("Test-Timestamp-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32ListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Int64NonListWithoutAutoIncrementWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Int64ListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int64 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Int64-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val JsonNonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = io.circe.parser.parse("""{"a":"a","b":1}""").toOption.map(PropertyDefaultValue.Object),
    description = Some("Test Json NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Json-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Json, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimestampListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Timestamp List WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-List-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(true)),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32NonListWithoutAutoIncrementWithDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Float64ListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 List WithoutDefaultValue NonNullable Description"),
    name = Some("Test-Float64-List-WithoutDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyNonListWithDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("defaultTextValue")),
      description = Some("Test TextProperty NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-TextProperty-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.TextProperty(Some(false), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val EnumPropertyNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Enum EnumProperty NonList WithoutDefaultValue Nullable Description"),
      name = Some("Enum-EnumProperty-NonList-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.EnumProperty(
        values = Map(
          "VAL1" -> EnumValueMetadata(Some("value1"), Some("value 1")),
          "VAL2" -> EnumValueMetadata(None, None)
        ),
        unknownValue = Some("VAL2")
      ),
      container = None,
      containerPropertyIdentifier = None
    )
  val EnumPropertyNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Enum EnumProperty NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Enum-EnumProperty-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.EnumProperty(
        values = Map(
          "VAL1" -> EnumValueMetadata(Some("value1"), Some("value 1")),
          "VAL2" -> EnumValueMetadata(None, None)
        ),
        unknownValue = Some("VAL2")
      ),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int64NonListWithAutoIncrementWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val Int64NonListWithAutoIncrementWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
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

  val TimestampNonListWithDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845609Z[UTC]")),
      description = Some("Test Timestamp NonList WithDefaultValue NonNullable Description"),
      name = Some("Test-Timestamp-NonList-WithDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val BooleanNonListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Boolean NonList WithoutDefaultValue Nullable Description"),
    name = Some("Test-Boolean-NonList-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithoutDefaultValueNonNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test Boolean NonList WithoutDefaultValue NonNullable Description"),
      name = Some("Test-Boolean-NonList-WithoutDefaultValue-NonNullable-Name"),
      `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
      container = None,
      containerPropertyIdentifier = None
    )

  val TimestampNonListWithDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.String("2023-01-17T20:39:57.845367Z[UTC]")),
    description = Some("Test Timestamp NonList WithDefaultValue Nullable Description"),
    name = Some("Test-Timestamp-NonList-WithDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Timestamp, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TextPropertyListWithoutDefaultValueNullable: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(true),
      autoIncrement = Some(false),
      defaultValue = None,
      description = Some("Test TextProperty List WithoutDefaultValue Nullable Description"),
      name = Some("Test-TextProperty-List-WithoutDefaultValue-Nullable-Name"),
      `type` = PropertyType.TextProperty(Some(true), Some("ucs_basic")),
      container = None,
      containerPropertyIdentifier = None
    )

  val Int32ListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Int32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Int32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Int32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32ListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float32 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float32-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val BooleanNonListWithDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Boolean(false)),
    description = Some("Test Boolean NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Boolean-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Boolean, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float64ListWithoutDefaultValueNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(true),
    autoIncrement = Some(false),
    defaultValue = None,
    description = Some("Test Float64 List WithoutDefaultValue Nullable Description"),
    name = Some("Test-Float64-List-WithoutDefaultValue-Nullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float64, Some(true)),
    container = None,
    containerPropertyIdentifier = None
  )

  val Float32NonListWithDefaultValueNonNullable: ViewCorePropertyDefinition = ViewCorePropertyDefinition(
    nullable = Some(false),
    autoIncrement = Some(false),
    defaultValue = Some(PropertyDefaultValue.Float32(1.2F)),
    description = Some("Test Float32 NonList WithDefaultValue NonNullable Description"),
    name = Some("Test-Float32-NonList-WithDefaultValue-NonNullable-Name"),
    `type` = PropertyType.PrimitiveProperty(PrimitivePropType.Float32, Some(false)),
    container = None,
    containerPropertyIdentifier = None
  )

  val TimeSeriesReference: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.TimeSeriesReference("timeseries1")),
      description = Some("Test Time Series Description"),
      name = Some("Test-Time-Series-Name"),
      `type` = PropertyType.TimeSeriesReference(),
    )

  val FileReference: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.FileReference("file1")),
      description = Some("Test File Description"),
      name = Some("Test-File-Name"),
      `type` = PropertyType.FileReference(),
    )

  val SequenceReference: ViewCorePropertyDefinition =
    ViewCorePropertyDefinition(
      nullable = Some(false),
      autoIncrement = Some(false),
      defaultValue = Some(PropertyDefaultValue.SequenceReference("sequence1")),
      description = Some("Test Sequence Description"),
      name = Some("Test-Sequence-Name"),
      `type` = PropertyType.SequenceReference(),
    )

  val AllPossibleViewPropertyDefs: Vector[ViewCorePropertyDefinition] = Vector(
    EnumPropertyNonListWithoutDefaultValueNonNullable,
    EnumPropertyNonListWithoutDefaultValueNullable,
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
    Float32NonListWithDefaultValueNonNullable,
    TimeSeriesReference,
    FileReference,
    SequenceReference
  )
}
