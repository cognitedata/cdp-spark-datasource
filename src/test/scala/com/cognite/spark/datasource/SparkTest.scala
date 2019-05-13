package com.cognite.spark.datasource

import cats.effect.IO
import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

import scala.concurrent.TimeoutException

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")
object GreenfieldTest extends Tag("GreenfieldTest")

trait SparkTest extends CdpConnector {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()

  def getThreeDModelIdAndRevisionId(auth: Auth): (String, String) = {
    val project = getProject(auth, 10, Constants.DefaultBaseUrl)

    val modelUrl = uri"https://api.cognitedata.com/api/0.6/projects/$project/3d/models"
    val models = getJson[Data[Items[ModelItem]]](auth, modelUrl, 10).unsafeRunSync()
    val modelId = models.data.items.head.id.toString

    val revisionsUrl = uri"https://api.cognitedata.com/api/0.6/projects/$project/3d/models/$modelId/revisions"
    val revisions = getJson[Data[Items[ModelRevisionItem]]](auth, revisionsUrl, 10).unsafeRunSync()
    val revisionId = revisions.data.items.head.id.toString

    (modelId, revisionId)
  }

  def retryWhile[A](action: => A, shouldRetry: A => Boolean): A =
    retryWithBackoff(
      IO {
        val actionValue = action
        if (shouldRetry(actionValue)) {
          throw new TimeoutException("Retry")
        }
        actionValue
      },
      Constants.DefaultInitialRetryDelay,
      Constants.DefaultMaxRetries
    ).unsafeRunSync()
}
