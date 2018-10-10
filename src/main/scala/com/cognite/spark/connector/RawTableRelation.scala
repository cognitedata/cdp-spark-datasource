package com.cognite.spark.connector

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.JsonObject
import okhttp3._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
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

  override val schema: StructType = userSchema.getOrElse {
    if (inferSchema) {
      val rdd = readRows(inferSchemaLimit)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf = renameKeyColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
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

  private val temporaryKeyName = s"TrE85tFQPCb2fEUZ"

  override def buildScan(): RDD[Row] = {
    val rdd = readRows(limit)
    if (schema == defaultSchema || schema == null || schema.tail.isEmpty) {
      rdd
    } else {
      val jsonFields = schemaWithoutRenamedKeyColumns(StructType.apply(schema.tail))
      val df = sqlContext.sparkSession.createDataFrame(rdd, defaultSchema)
      import sqlContext.implicits.StringToColumn
      import org.apache.spark.sql.functions.from_json
      val dfWithSchema = df.select($"key", from_json($"columns", jsonFields).alias("columns"))
      val flatDf = if (keyColumns(jsonFields).isEmpty) {
        dfWithSchema.select("key", "columns.*")
      } else {
        val dfWithKeyRenamed = dfWithSchema.withColumnRenamed("key", temporaryKeyName)
        val temporaryFlatDf = renameKeyColumns(dfWithKeyRenamed.select(temporaryKeyName, "columns.*"))
        temporaryFlatDf.withColumnRenamed(temporaryKeyName, "key")
      }
      flatDf.rdd
    }
  }

  override def insert(df: org.apache.spark.sql.DataFrame, overwrite: scala.Boolean): scala.Unit = {
    if (!df.columns.contains("key")) {
      throw new IllegalArgumentException("The dataframe used for insertion must have a \"key\" column")
    }

    val dfWithKeyRenamed = df.withColumnRenamed("key", temporaryKeyName)
    val dfWithUnRenamedKeyColumns = unRenameKeyColumns(dfWithKeyRenamed)
    val columnNames = dfWithUnRenamedKeyColumns.columns.filter(!_.equals(temporaryKeyName))
    dfWithUnRenamedKeyColumns.foreachPartition(rows => {
      rows.grouped(batchSize).foreach(postRows(columnNames, _))
    })
  }

  private def postRows(nonKeyColumnNames: Array[String], rows: Seq[Row]) = {
    val items = rows.map(row => RawItem(
      row.getString(row.fieldIndex(temporaryKeyName)),
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
