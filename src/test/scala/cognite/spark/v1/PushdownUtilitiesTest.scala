package cognite.spark.v1

import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}
import sttp.client3._
import PushdownUtilities._
class PushdownUtilitiesTest extends FlatSpec with ParallelTestExecution with Matchers with SparkTest {

  it should "create one request for 1x1 and expression" in {
    val baseUri = uri"https://api.com"
    val pushdownExpression = PushdownAnd(PushdownFilter("id", "123"), PushdownFilter("type", "abc"))
    val params = pushdownToParameters(pushdownExpression)

    assert(params.length == 1)
  }

  it should "create two requests for 1+1 or expression" in {
    val baseUri = uri"https://api.com"
    val pushdownExpression =
      PushdownFilters(Seq(PushdownFilter("id", "123"), PushdownFilter("type", "abc")))
    val params = pushdownToParameters(pushdownExpression)

    assert(params.length == 2)
  }

  it should "create 9 requests for 3x3 and or expression" in {
    val baseUri = uri"https://api.com"
    val left = PushdownFilters(
      Seq(
        PushdownFilter("id", "123"),
        PushdownFilter("type", "abc"),
        PushdownFilter("description", "test")))
    val right = PushdownFilters(
      Seq(
        PushdownFilter("id", "456"),
        PushdownFilter("type", "def"),
        PushdownFilter("description", "test2")))
    val pushdownExpression = PushdownAnd(left, right)
    val params = pushdownToParameters(pushdownExpression)

    assert(params.length == 9)
  }
}
