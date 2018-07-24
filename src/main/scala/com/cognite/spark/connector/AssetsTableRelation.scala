package com.cognite.spark.connector

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

case class PostAssetsDataItems[A](items: Seq[A])

case class AssetsItem(name: String,
                      // FIXME: parentId is optional
                      parentId: Long,
                      description: String,
                      metadata: Map[String, String],
                      id: Long)

case class PostAssetsItem(name: String,
                          description: String,
                          metadata: Map[String, String])

// TODO: there's obviously a lot of copy-paste going on here at the moment,
// and this should be refactored once we've identified some common patterns.
class AssetsTableRelation(apiKey: String,
                          project: String,
                          assetPath: Option[String],
                          limit: Option[Int],
                          batchSizeOption: Option[Int])(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {
  // TODO: make read/write timeouts configurable
  @transient lazy val client: OkHttpClient = new OkHttpClient.Builder()
    .readTimeout(2, TimeUnit.MINUTES)
    .writeTimeout(2, TimeUnit.MINUTES)
    .build()
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    mapper
  }
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)

  override def schema: StructType = StructType(Seq(
    StructField("name", DataTypes.StringType),
    StructField("parentId", DataTypes.LongType),
    StructField("description", DataTypes.StringType),
    StructField("metadata", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
    StructField("id", DataTypes.LongType)))

  override def buildScan(): RDD[Row] = {
    val urlBuilder = AssetsTableRelation.baseAssetsURL(project)
    assetPath.foreach(path => urlBuilder.addQueryParameter("path", path))
    val result = CdpResults[AssetsItem](apiKey, urlBuilder.build(), batchSize, limit)
      .map(item => Row(item.name, item.parentId, item.description, item.metadata, item.id))
      .toList

    sqlContext.sparkContext.parallelize(result)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    df.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows)
    })
  }

  private def postRows(rows: Seq[Row]) = {
    // this definitely seems like a common pattern that could be refactored out to a common method
    val assetItems = rows.map(r =>
      PostAssetsItem(r.getString(0), r.getString(2), r.getAs[Map[String, String]](3)))
    val items = PostAssetsDataItems[PostAssetsItem](assetItems)

    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, mapper.writeValueAsString(items))
    println("Posting '" + mapper.writeValueAsString(items) + "' to " + AssetsTableRelation.baseAssetsURL(project).build().toString)
    val response = client.newCall(
      CdpConnector.baseRequest(apiKey)
        .url(AssetsTableRelation.baseAssetsURL(project).build())
        .post(requestBody)
        .build()
    ).execute()
    if (!response.isSuccessful) {
      throw new RuntimeException("Non-200 status when posting to raw API, received " + response.code() + "(" + response.message() + ")")
    }
  }
}

object AssetsTableRelation {
  val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
    mapper
  }

  def baseAssetsURL(project: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.4")
      .addPathSegment("assets")
  }

  val validPathComponentTypes = Seq(
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.String])

  def isValidAssetsPath(path: String): Boolean = {
    try {
      val pathComponents = mapper.readValue(path, classOf[Seq[Any]])
      pathComponents.forall(c => validPathComponentTypes.contains(c.getClass))
    } catch {
      case _: JsonParseException => false
    }
  }
}
