package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import org.apache.spark.sql.types.DataType

@deprecated("wdl support is deprecated", since = "0")
class RequiredFieldIsNullException(val structFieldName: String, val dataType: DataType)
    extends CdfSparkException(
      s"Required field `${structFieldName}` of type `${dataType.typeName}` should not be NULL.")

@deprecated("wdl support is deprecated", since = "0")
class WrongFieldTypeException(
    val structFieldName: String,
    val expectedDataType: DataType,
    val value: Any)
    extends CdfSparkException(
      s"Field `$structFieldName` with expected type `$expectedDataType` contains invalid value: `$value`."
    )
