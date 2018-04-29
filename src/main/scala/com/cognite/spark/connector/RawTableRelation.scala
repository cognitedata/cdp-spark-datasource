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
                       limit: Option[Int],
                       inferSchema: Boolean,
                       inferSchemaLimit: Option[Int],
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
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)
  @transient lazy val defaultSchema = StructType(Seq(
    StructField("key", DataTypes.StringType),
    StructField("columns", DataTypes.StringType)
  ))

  override val schema: StructType = userSchema.getOrElse {
    if (inferSchema) {
      val rdd = readRows(inferSchemaLimit)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf = sqlContext.sparkSession.read.json(df.select($"columns").as[String])
      StructType(StructField("key", DataTypes.StringType) +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  private def readRows(limit: Option[Int]): RDD[Row] = {
    val responses: ListBuffer[Row] = ListBuffer()
    var doneReading = false
    var cursor: Option[String] = None
    var nRowsRemaining = limit

    do {
      val thisBatchSize = scala.math.min(nRowsRemaining.getOrElse(batchSize), batchSize)
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
          throw new RuntimeException("Non-200 status when querying API for " +
            "database " + database + ", table " + table + " in project " + project)
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

  override def buildScan(): RDD[Row] = {
    val rdd = readRows(limit)
    if (schema == defaultSchema || schema == null || schema.tail.isEmpty) {
      rdd
    } else {
      val jsonFields = StructType.apply(schema.tail)
      val df = sqlContext.sparkSession.createDataFrame(rdd, defaultSchema)
      import sqlContext.implicits.StringToColumn
      import org.apache.spark.sql.functions._
      val dfWithSchema = df.select($"key", from_json($"columns", jsonFields).alias("columns"))
      val flatDf = dfWithSchema.select("key", "columns.*")
      flatDf.rdd
    }
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
