package com.cognite.spark.datasource

import java.util.concurrent.{CountDownLatch, TimeUnit}

import cats.effect.IO
import com.cognite.spark.datasource.CdpConnector.DataItemsWithCursor
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
import com.cognite.spark.datasource.Tap._
import cats.implicits._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

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

  private def readRows(limit: Option[Int], collectMetrics: Boolean = collectMetrics): RDD[Row] = {
    val url = baseRawTableURL(project, database, table).addQueryParameter("columns", ",").build()
    val cursors = CdpConnector.getWithCursor[RawItem](apiKey, url, batchSize, limit)
      .map(chunkWithCursor => (chunkWithCursor.chunk.length, chunkWithCursor.cursor))
      .toSeq

    val keysRdd = sqlContext.sparkContext.parallelize(scala.util.Random.shuffle(cursors))
    keysRdd.flatMap { key =>
      CdpConnector.get[RawItem](apiKey, baseRawTableURL(project, database, table).build(),
        batchSize, limit = Some(key._1), initialCursor = key._2)
        .map(tap(_ => if (collectMetrics) {
          rowsRead.inc()
        }))
        .map(item => Row(item.key, item.columns.asJson.noSpaces))
    }
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
      val batches = rows.grouped(batchSize).toVector
      val batchPosts = fs2.async.parallelTraverse(batches)(postRows(columnNames, _))
      batchPosts.unsafeRunSync()
    })
  }

  private def rowsToRawItems(nonKeyColumnNames: Seq[String], rows: Seq[Row]): Seq[RawItem] = {
    rows.map(row => RawItem(
      row.getString(row.fieldIndex(temporaryKeyName)),
      io.circe.parser.decode[JsonObject](
        mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames))).right.get
    ))
  }

  private def postRows(nonKeyColumnNames: Seq[String], rows: Seq[Row]): IO[Unit] = {
    val items = rowsToRawItems(nonKeyColumnNames, rows)

    val url = baseRawTableURL(project, database, table)
      .addPathSegment("create")
      .build()

    CdpConnector.post(apiKey, url, items)
      .map(tap(_ =>
        if (collectMetrics) {
          rowsCreated.inc(rows.length)
        }
      ))
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
