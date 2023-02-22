package cognite.spark.v1

import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.compat.immutable.ArraySeq.unsafeWrapArray

trait DataFrameMatcher extends Matchers {
  def containTheSameRowsAs(expectedDataFrame: DataFrame): ExpectedDataFrameMatcher =
    new ExpectedDataFrameMatcher(expectedDataFrame)

  class ExpectedDataFrameMatcher(expectedDataFrame: DataFrame) extends Matcher[DataFrame] {
    def apply(dataFrame: DataFrame): MatchResult =
      if (dataFrame.columns.toSet == expectedDataFrame.columns.toSet) {
        val leftOuter = dataFrame
          .select(
            unsafeWrapArray(expectedDataFrame.columns.map(dataFrame(_))): _*
          )
          .exceptAll(expectedDataFrame)
          .isEmpty

        val rightOuter = expectedDataFrame
          .select(unsafeWrapArray(dataFrame.columns.map(expectedDataFrame(_))): _*)
          .exceptAll(dataFrame)
          .isEmpty

        if (leftOuter && rightOuter) {
          MatchResult(
            matches = true,
            "",
            s"Actual dataframe [${dataFrame.collect().mkString("Array(", ", ", ")")}] is equal to expected: [${expectedDataFrame.collect().mkString("Array(", ", ", ")")}]"
          )
        } else {
          MatchResult(
            matches = false,
            s"Actual dataframe ${dataFrame.collect().mkString("Array(", ", ", ")")} is different from expected: [${
              expectedDataFrame
                .collect()
                .mkString("Array(", ", ", ")")
            }]",
            ""
          )
        }
      } else {
        MatchResult(
          matches = false,
          s"Actual dataframe columns [${dataFrame.columns.sorted.mkString("Array(", ", ", ")")}] is different from expected: [${
            expectedDataFrame.columns.sorted
              .mkString("Array(", ", ", ")")
          }]",
          ""
        )
      }
  }
}
