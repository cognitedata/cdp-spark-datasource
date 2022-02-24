package cognite.spark.v1

import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class DataModelInstanceRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  import spark.implicits._

}