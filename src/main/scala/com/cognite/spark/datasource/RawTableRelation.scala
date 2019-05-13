package com.cognite.spark.datasource

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.softwaremill.sttp._
import io.circe.JsonObject
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.datasource._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext

case class RawItem(key: String, columns: JsonObject)

class RawTableRelation(
    config: RelationConfig,
    database: String,
    table: String,
    userSchema: Option[StructType],
    inferSchema: Boolean,
    inferSchemaLimit: Option[Int],
    collectSchemaInferenceMetrics: Boolean)(val sqlContext: SQLContext)
    extends BaseRelation
    with InsertableRelation
    with TableScan
    with CdpConnector
    with Serializable {
  import RawTableRelation._

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)
  @transient lazy private val maxRetries = config.maxRetries

  @transient lazy val defaultSchema = StructType(
    Seq(
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

  @transient lazy private val rowsCreated =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"raw.$database.$table.rows.created")
  @transient lazy private val rowsRead =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"raw.$database.$table.rows.read")

  override val schema: StructType = userSchema.getOrElse {
    if (inferSchema) {
      val rdd = readRows(inferSchemaLimit, collectSchemaInferenceMetrics)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf =
        renameKeyColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
      StructType(StructField("key", DataTypes.StringType, false) +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  private def readRows(
      limit: Option[Int],
      collectMetrics: Boolean = config.collectMetrics): RDD[Row] = {
    val baseUrl = baseRawTableURL(config.project, database, table)
    CdpRdd[RawItem](
      sqlContext.sparkContext,
      (item: RawItem) => {
        if (collectMetrics) {
          rowsRead.inc()
        }
        Row(item.key, item.columns.asJson.noSpaces)
      },
      baseUrl,
      config,
      new NextCursorIterator[RawItem](baseUrl.param("columns", ","), config)
    )
  }

  override def buildScan(): RDD[Row] = {
    val rdd = readRows(config.limit)
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
      throw new IllegalArgumentException(
        "The dataframe used for insertion must have a \"key\" column.")
    }

    val (columnNames, dfWithUnRenamedKeyColumns) = prepareForInsert(df)
    dfWithUnRenamedKeyColumns.foreachPartition((rows: Iterator[Row]) => {
      implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      val batches = rows.grouped(batchSize).toVector
      batches.grouped(Constants.MaxConcurrentRequests).foreach { batchGroup =>
        batchGroup.parTraverse(postRows(columnNames, _)).unsafeRunSync()
      }
      ()
    })
  }

  private def postRows(nonKeyColumnNames: Seq[String], rows: Seq[Row]): IO[Unit] = {
    val items = rowsToRawItems(nonKeyColumnNames, rows, mapper)

    val url = uri"${baseRawTableURL(config.project, database, table)}/create"

    post(config.auth, url, items, maxRetries)
      .flatTap { _ =>
        IO {
          if (config.collectMetrics) {
            rowsCreated.inc(rows.length)
          }
        }
      }
  }

  def baseRawTableURL(project: String, database: String, table: String): Uri =
    uri"${baseUrl(project, "0.5", config.baseUrl)}/raw/$database/$table"
}

object RawTableRelation {
  private val keyColumnPattern = """^_*key$""".r

  private def keyColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(keyColumnPattern.findFirstIn(_).isDefined)

  def rowsToRawItems(
      nonKeyColumnNames: Seq[String],
      rows: Seq[Row],
      mapper: ObjectMapper): Seq[RawItem] =
    rows.map(
      row =>
        RawItem(
          Option(row.getString(row.fieldIndex(temporaryKeyName)))
            .getOrElse(throw new IllegalArgumentException("\"key\" can not be null.")),
          io.circe.parser
            .decode[JsonObject](mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames)))
            .right
            .get
      ))

  private def schemaWithoutRenamedKeyColumns(schema: StructType) =
    StructType.apply(schema.fields.map(field => {
      if (keyColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else {
        field
      }
    }))

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

  def flattenAndRenameKeyColumns(
      sqlContext: SQLContext,
      df: DataFrame,
      jsonFieldsSchema: StructType): DataFrame = {
    import org.apache.spark.sql.functions.from_json
    import sqlContext.implicits.StringToColumn
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
}
