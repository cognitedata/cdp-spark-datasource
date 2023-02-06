package cognite.spark.v1.wdl

import org.apache.spark.sql.Row
import org.scalactic.Equality

private[wdl] object RowEquality { // scalastyle:ignore object.name
  implicit val rowEq: Equality[Row] =
    new Equality[Row] {
      def areEqual(a: Row, b: Any): Boolean =
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
}
