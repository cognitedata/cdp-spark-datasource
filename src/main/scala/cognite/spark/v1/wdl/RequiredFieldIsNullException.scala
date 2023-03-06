package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import org.apache.spark.sql.types.DataType

class RequiredFieldIsNullException(val structFieldName: String, val dataType: DataType)
    extends CdfSparkException(
      s"Required field `${structFieldName}` of type `${dataType.typeName}` should not be NULL.")
