package com.cognite.spark.datasource

import com.softwaremill.sttp._
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

object ReadTest extends Tag("ReadTest")
object WriteTest extends Tag("WriteTest")

trait SparkTest extends CdpConnector {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", this.getClass.getName + math.floor(math.random * 1000).toLong.toString)
    .getOrCreate()

  def getThreeDModelIdAndRevisionId(apiKey: String): (String, String) = {
    val project = getProject(apiKey, 10)

    val modelUrl = uri"https://api.cognitedata.com/api/0.6/projects/$project/3d/models"
    val models = getJson[Data[Items[ModelItem]]](apiKey, modelUrl, 10).unsafeRunSync()
    val modelId = models.data.items.head.id.toString

    val revisionsUrl = uri"https://api.cognitedata.com/api/0.6/projects/$project/3d/models/$modelId/revisions"
    val revisions = getJson[Data[Items[ModelRevisionItem]]](apiKey, revisionsUrl, 10).unsafeRunSync()
    val revisionId = revisions.data.items.head.id.toString

    (modelId, revisionId)
  }
}
