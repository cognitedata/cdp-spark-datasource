package cognite.spark.v1

import org.apache.spark.sql.types.StructType

trait StructTypeEncoder[T] {
  def structType(): StructType
}
