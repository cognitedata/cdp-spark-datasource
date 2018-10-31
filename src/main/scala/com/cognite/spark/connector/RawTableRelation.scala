package com.cognite.spark.connector

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.cognite.spark.connector.CdpConnector.{DataItemsWithCursor, callWithRetries, client, reportResponseFailure}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.JsonObject
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import io.circe.generic.auto._
import io.circe.parser.decode
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
                       collectMetrics: Boolean,
                       collectSchemaInferenceMetrics: Boolean)(val sqlContext: SQLContext)
  extends BaseRelation
    with InsertableRelation
    with TableScan
    with Serializable {
  import RawTableRelation._

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
      val rdd = readRows(inferSchemaLimit, collectSchemaInferenceMetrics)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf = renameKeyColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
      StructType(StructField("key", DataTypes.StringType) +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  private def getRows(requestBuilder: Request.Builder, collectMetrics: Boolean) = {
    val response = callWithRetries(client.newCall(requestBuilder.build()), 5)
    if (!response.isSuccessful) {
      reportResponseFailure(requestBuilder.build().url(), s"received ${response.code()} (${response.message()})")
    }
    try {
      val d = response.body().string()
      decode[DataItemsWithCursor[RawItem]](d) match {
        case Right(r) =>
          if (collectMetrics) {
            rowsRead.inc(r.data.items.length)
          }
          r.data.items
        case Left(e) => throw new RuntimeException("Failed to deserialize", e)
      }
    } finally {
      response.close()
    }
  }

  private def readRows(limit: Option[Int], collectMetrics: Boolean = collectMetrics): RDD[Row] = {
    val url = baseRawTableURL(project, database, table).addQueryParameter("columns", ",").build()
    var moreKeys = true
    var cursor: Option[String] = None
    var cursors = List[(Option[String], Int)]()
    var itemsRemaining = limit

    while (moreKeys && itemsRemaining.forall(_ > 0)) {
      val limitValue = itemsRemaining.map(Math.min(batchSize, _)).getOrElse(batchSize)
      val nextUrl = url.newBuilder().addQueryParameter("limit", limitValue.toString)
      cursor.foreach(cur => nextUrl.addQueryParameter("cursor", cur))
      val requestBuilder = CdpConnector.baseRequest(apiKey)
      val response = callWithRetries(client.newCall(requestBuilder.url(nextUrl.build()).build()), 5)
      try {
        val d = response.body().string()
        decode[DataItemsWithCursor[RawItem]](d) match {
          case Right(r) =>
            cursors = (cursor, r.data.items.length) :: cursors
            itemsRemaining = itemsRemaining.map(_ - r.data.items.length)
            r.data.nextCursor match {
              case nextCursor @ Some(_) => cursor = nextCursor
              case None => moreKeys = false
            }
          case Left(e) => throw new RuntimeException("Failed to deserialize", e)
        }
      } finally {
        response.close()
      }
    }

    val keysRdd = sqlContext.sparkContext.parallelize(scala.util.Random.shuffle(cursors))
    keysRdd.flatMap(key =>
      getRows(CdpConnector.baseRequest(apiKey)
        .url(key match {
          case (Some(cursor), numItems) =>
            baseRawTableURL(project, database, table)
              .addQueryParameter("limit", numItems.toString)
              .addQueryParameter("cursor", cursor)
              .build()
          case (None, numItems) =>
            baseRawTableURL(project, database, table)
              .addQueryParameter("limit", numItems.toString)
              .build()
        }), collectMetrics
      ).map(item => Row(item.key, item.columns.asJson.noSpaces)))
  }

  override def buildScan(): RDD[Row] = {
    val rdd = readRows(limit)
    if (schema == defaultSchema || schema == null || schema.tail.isEmpty) {
      rdd
    } else {
      val jsonFieldsSchema = schemaWithoutRenamedKeyColumns(StructType.apply(schema.tail))
      val df = sqlContext.sparkSession.createDataFrame(rdd, defaultSchema)
      flattenAndRenameKeyColumns(sqlContext, df, jsonFieldsSchema).rdd
    }
  }

  override def insert(df: DataFrame, overwrite: scala.Boolean): scala.Unit = {
    if (!df.columns.contains("key")) {
      throw new IllegalArgumentException("The dataframe used for insertion must have a \"key\" column")
    }

    val (columnNames, dfWithUnRenamedKeyColumns) = prepareForInsert(df)
    dfWithUnRenamedKeyColumns.foreachPartition(rows => {
      val batches = rows.grouped(batchSize).toSeq
      val remainingRequests = new CountDownLatch(batches.length)
      batches.foreach(postRows(columnNames, _, remainingRequests))
      remainingRequests.await(10, TimeUnit.SECONDS)
    })
  }

  private def rowsToRawItems(nonKeyColumnNames: Seq[String], rows: Seq[Row]): Seq[RawItem] = {
    rows.map(row => RawItem(
      row.getString(row.fieldIndex(temporaryKeyName)),
      io.circe.parser.decode[JsonObject](
        mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames))).right.get
    ))
  }

  private def postRows(nonKeyColumnNames: Seq[String], rows: Seq[Row], remainingRequests: CountDownLatch) = {
    val items = rowsToRawItems(nonKeyColumnNames, rows)

    val url = baseRawTableURL(project, database, table)
      .addPathSegment("create")
      .build()

    CdpConnector.post(apiKey, url, items, true,
      successCallback =  Some(_ => {
        if (collectMetrics) {
          rowsCreated.inc(rows.length)
        }
        remainingRequests.countDown()
      }),
      failureCallback = Some(_ => remainingRequests.countDown())
    )
  }
}

object RawTableRelation {
  private val keyColumnPattern = """^_*key$""".r

  private def keyColumns(schema: StructType): Array[String] = {
    schema.fieldNames.filter(keyColumnPattern.findFirstIn(_).isDefined)
  }

  private def schemaWithoutRenamedKeyColumns(schema: StructType) = {
    StructType.apply(schema.fields.map(field => {
      if (keyColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else {
        field
      }
    }))
  }

  private def renameKeyColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema)
    // rename key columns starting with the longest one first, to avoid creating a column with the same name
    columnsToRename.sorted.foldLeft(df) { (df, keyColumn) =>
      df.withColumnRenamed(keyColumn, s"_$keyColumn")
    }
  }

  private def unRenameKeyColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema)
    // when renaming them back we instead start with the shortest key column name, for similar reasons
    columnsToRename.sortWith(_ > _).foldLeft(df) { (df, keyColumn) =>
      df.withColumnRenamed(keyColumn, keyColumn.substring(1))
    }
  }

  private val temporaryKeyName = s"TrE85tFQPCb2fEUZ"

  def flattenAndRenameKeyColumns(sqlContext: SQLContext, df: DataFrame, jsonFieldsSchema: StructType): DataFrame = {
    import sqlContext.implicits.StringToColumn
    import org.apache.spark.sql.functions.from_json
    val dfWithSchema = df.select($"key", from_json($"columns", jsonFieldsSchema).alias("columns"))

    if (keyColumns(jsonFieldsSchema).isEmpty) {
      dfWithSchema.select("key", "columns.*")
    } else {
      val dfWithKeyRenamed = dfWithSchema.withColumnRenamed("key", temporaryKeyName)
      val temporaryFlatDf = renameKeyColumns(dfWithKeyRenamed.select(temporaryKeyName, "columns.*"))
      temporaryFlatDf.withColumnRenamed(temporaryKeyName, "key")
    }
  }

  def prepareForInsert(df: DataFrame): (Seq[String], DataFrame) = {
    val dfWithKeyRenamed = df.withColumnRenamed("key", temporaryKeyName)
    val dfWithUnRenamedKeyColumns = unRenameKeyColumns(dfWithKeyRenamed)
    val columnNames = dfWithUnRenamedKeyColumns.columns.filter(!_.equals(temporaryKeyName))
    (columnNames, dfWithUnRenamedKeyColumns)
  }

  def baseRawTableURL(project: String, database: String, table: String): HttpUrl.Builder = {
    CdpConnector.baseUrl(project, "0.5")
      .addPathSegment("raw")
      .addPathSegment(database)
      .addPathSegment(table)
  }
}
