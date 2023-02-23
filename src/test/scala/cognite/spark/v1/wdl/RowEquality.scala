package cognite.spark.v1.wdl

import org.apache.spark.sql.Row
import org.scalactic.Equality

private[wdl] object RowEquality {
  implicit val rowEq: Equality[Row] =
    (a: Row, b: Any) =>
      b match {
        case p: Row =>
          val fieldNames = a.schema.fieldNames
          if (fieldNames.toSet == p.schema.fieldNames.toSet) {
            a.getValuesMap(fieldNames.toSeq) == p.getValuesMap(fieldNames.toSeq)
          } else {
            false
          }
        case _ => false
    }
}
