package com.cognite.spark.connector

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.JsonObject
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.groupon.metrics.UserMetricsSystem

case class RawItem(key: String, columns: JsonObject)

class RawTableRelation(apiKey: String,
                       project: String,
                       database: String,
                       table: String,
                       userSchema: Option[StructType],
                       limit: Option[Int],
                       inferSchema: Boolean,
                       inferSchemaLimit: Option[Int],
                       batchSizeOption: Option[Int],
                       metricsPrefix: String,
                       collectMetrics: Boolean)(val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {
  // TODO: make read/write timeouts configurable
  @transient lazy val batchSize = batchSizeOption.getOrElse(10000)
  @transient lazy val defaultSchema = StructType(Seq(
    StructField("key", DataTypes.StringType),
    StructField("columns", DataTypes.StringType)
  ))
  @transient lazy val mapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    mapper
  }

  // TODO: check if we need to sanitize the database and table names, or if they are reasonably named
  lazy private val rowsCreated = UserMetricsSystem.counter(s"${metricsPrefix}raw.$database.$table.rows.created")
  lazy private val rowsRead = UserMetricsSystem.counter(s"${metricsPrefix}raw.$database.$table.rows.read")

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

  private def countItems(items: CdpConnector.DataItemsWithCursor[_]): Unit = {
    rowsRead.inc(items.data.items.length)
  }

  private def readRows(limit: Option[Int]): RDD[Row] = {
    val url = RawTableRelation.baseRawTableURL(project, database, table).build()
    val result = CdpConnector.get[RawItem](apiKey, url, batchSize, limit,
      batchCompletedCallback = if (collectMetrics) Some(countItems) else None)
      .map(item => Row(item.key, item.columns.asJson.noSpaces))
    sqlContext.sparkContext.parallelize(result.toStream)
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
    val items = rows.map(row => RawItem(
      row.getString(row.fieldIndex("key")),
      io.circe.parser.decode[JsonObject](
        mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames))).right.get
      ))

    val url = RawTableRelation.baseRawTableURL(project, database, table)
      .addPathSegment("create")
      .build()

    CdpConnector.post(apiKey, url, items)
    if (collectMetrics) {
      rowsCreated.inc(items.length)
    }
  }
}

object RawTableRelation {
  def baseRawTableURL(project: String, database: String, table: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.4")
      .addPathSegment("raw")
      .addPathSegment(database)
      .addPathSegment(table)
  }
}
