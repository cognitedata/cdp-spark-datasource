package com.cognite.spark.connector

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

case class RawData(data: RawDataItems)

case class RawDataItems(items: Seq[RawDataItemsItem], nextCursor: Option[String] = None)

case class RawDataItemsItem(key: String, columns: Map[String, Any])

class RawTableRelation(apiKey: String,
                       project: String,
                       database: String,
                       table: String,
                       userSchema: Option[StructType],
                       limit: Option[Long],
                       batchSizeOption: Option[Int])(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {

  val DEFAULT_BATCH_SIZE = 10000

  // TODO: make read/write timeouts configurable
  @transient lazy val client: OkHttpClient = new OkHttpClient.Builder()
    .readTimeout(2, TimeUnit.MINUTES)
    .writeTimeout(2, TimeUnit.MINUTES)
    .build()
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }
  val batchSize = batchSizeOption match {
    case Some(size) => size
    case None => DEFAULT_BATCH_SIZE
  }

  override def schema: StructType = userSchema.getOrElse[StructType] {
    // not sure if we should flatten here or not. probably yes? it's a much nicer interface, but it does have a
    // performance impact since we need some rows (not necessarily all) to do the schema inference.
    //
    // the procedure to do so is like this
    // step 1: read df, remember to cache()
    //    val df = spark.sqlContext.read.format("com.cognite.spark.connector").option("project", "akerbp").option("apiKey", apikey).option("type", "tables").option("database", "Workmate").load("TagDocument").cache()
    // step 2: infer json schema from columns
    //    val jsonDf = spark.read.json(df.select("columns").as[String])
    // step 3: apply schema to columns:
    //    val nonflatDf = df.select($"key", from_json($"columns", jsonDf.schema).alias("columns"))
    // step 4: flatten with json schema:
    //    val flatDf = nonflatDf.select("key", "columns.*")
    //
    // maybe use the inferSchema parameter to decide, similar to the CSV reader?
    // to do it efficiently we'd need a way to get a random sample of N rows from the table,
    // which is not possible with the current API.
    StructType(Seq(
      StructField("key", DataTypes.StringType),
      StructField("columns", DataTypes.StringType)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val responses: ListBuffer[Row] = ListBuffer()
    var doneReading = false
    var cursor: Option[String] = None
    var nRowsRemaining = limit

    do {
      val thisBatchSize = scala.math.min(nRowsRemaining.getOrElse(batchSize.toLong).toInt, batchSize)
      val urlBuilder = RawTableRelation.baseRawTableURL(project, database, table)
        .addQueryParameter("limit", thisBatchSize.toString)
      if (!cursor.isEmpty) {
        urlBuilder.addQueryParameter("cursor", cursor.get)
      }

      var response: Response = null
      try {
        response = client.newCall(
          RawTableRelation.baseRequest(apiKey)
            .url(urlBuilder.build())
            .build())
          .execute()
        if (!response.isSuccessful) {
          throw new RuntimeException("Non-200 status when querying API, received " + response.code() + "(" + response.message() + ")")
        }
        val r = mapper.readValue(response.body().string(), classOf[RawData])
        for (item <- r.data.items) {
          responses += Row(item.key, mapper.writeValueAsString(item.columns))
        }

        cursor = r.data.nextCursor
        nRowsRemaining = nRowsRemaining.map(_ - r.data.items.size)
      } finally {
        if (response != null) {
          response.close()
        }
      }

    } while (!cursor.isEmpty && (nRowsRemaining.isEmpty || nRowsRemaining.get > 0))

    sqlContext.sparkContext.parallelize(responses)
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    // TODO: make it possible to configure the name of the column to be used as the key
    if (!df.columns.contains("key")) {
      throw new IllegalArgumentException("The dataframe used for insertion must have a \"key\" column")
    }

    val columnNames = df.columns.filter(!_.equals("key"))
    df.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows(columnNames, _))
    })
  }

  private def postRows(nonKeyColumnNames: Array[String], rows: Seq[Row]) = {
    val rawDataItemsItems = rows.map(row => RawDataItemsItem(
      row.getString(row.fieldIndex("key")),
      row.getValuesMap[Any](nonKeyColumnNames)))
    val items = new RawDataItems(rawDataItemsItems)

    val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
    val requestBody = RequestBody.create(jsonMediaType, mapper.writeValueAsString(items))
    // we could even make those calls async, maybe not worth it though
    println("Posting to " + RawTableRelation.baseRawTableURL(project, database, table).build().toString)
    val response = client.newCall(
      RawTableRelation.baseRequest(apiKey)
        .url(RawTableRelation.baseRawTableURL(project, database, table)
          .addPathSegment("create")
          .build())
        .post(requestBody)
        .build()
    ).execute()
    if (!response.isSuccessful) {
      throw new RuntimeException("Non-200 status when posting to raw API, received " + response.code() + "(" + response.message() + ")")
    }
  }
}

object RawTableRelation {
  def baseRawTableURL(project: String, database: String, table: String): HttpUrl.Builder = {
    new HttpUrl.Builder()
      .scheme("https")
      .host("api.cognitedata.com")
      .addPathSegments("api/0.4/projects")
      .addPathSegment(project)
      .addPathSegment("raw")
      .addPathSegment(database)
      .addPathSegment(table)
  }

  def baseRequest(apiKey: String): Request.Builder = {
    new Request.Builder()
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Charset", "utf-8")
      .header("api-key", apiKey)
  }
}
