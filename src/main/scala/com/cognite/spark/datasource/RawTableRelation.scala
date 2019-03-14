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
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.concurrent.ExecutionContext
import scala.util.Try

case class RawItem(key: String, lastUpdatedTime: Long, columns: JsonObject)
case class RawItemForPost(key: String, columns: JsonObject)

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
    with PrunedFilteredScan
    with CdpConnector
    with Serializable {
  import RawTableRelation._

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)

  @transient lazy val defaultSchema = StructType(
    Seq(
      StructField("key", DataTypes.StringType),
      StructField(lastChangedColName, DataTypes.LongType),
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
      val rdd =
        readRows(
          inferSchemaLimit.orElse(Some(Constants.DefaultInferSchemaLimit)),
          (None, None),
          collectSchemaInferenceMetrics)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf =
        renameColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
      StructType(
        StructField("key", DataTypes.StringType, false)
          +: StructField(lastChangedColName, DataTypes.LongType, true)
          +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  private def readRows(
      limit: Option[Int],
      lastChangedLimits: (Option[Long], Option[Long]),
      collectMetrics: Boolean = config.collectMetrics): RDD[Row] = {
    val baseUrl = baseRawTableURL(config.project, database, table)
    val configWithLimit = config.copy(limit = limit)

    val (minUpdatedTime, maxUpdatedTime) = lastChangedLimits
    val baseUrlWithParams = baseUrl.params(
      Map(
        "minLastUpdatedTime" -> minUpdatedTime.map(_.toString),
        "maxLastUpdatedTime" -> maxUpdatedTime.map(_.toString))
        .collect { case (k, Some(v)) => k -> v })

    CdpRdd[RawItem](
      sqlContext.sparkContext,
      (item: RawItem) => {
        if (collectMetrics) {
          rowsRead.inc()
        }
        Row(item.key, item.lastUpdatedTime, item.columns.asJson.noSpaces)
      },
      baseUrlWithParams,
      configWithLimit,
      Seq[PushdownFilter](),
      new NextCursorIterator[RawItem](
        baseUrlWithParams.param("columns", ","),
        configWithLimit
      )
    )
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  private def getLastChangedLimit(filter: Filter): Seq[Limit] =
    filter match {
      // The upper bound of lastChanged passed into Raw API represents a closed interval.
      case LessThan(lastChangedColName, value) => Seq(Max(value.toString.toLong))
      case LessThanOrEqual(lastChangedColName, value) => Seq(Max(value.toString.toLong))
      // The lower bound of lastChanged passed into Raw API represents an open interval.
      case GreaterThan(lastChangedColName, value) => Seq(Min(value.toString.toLong))
      case GreaterThanOrEqual(lastChangedColName, value) => Seq(Min(value.toString.toLong - 1))
      case And(f1, f2) => getLastChangedLimit(f1) ++ getLastChangedLimit(f2)
      // case Or(f1, f2) => Ignored for now. See DataPointsRelation getTimestampLimit for explanation,
      case _ => Seq()
    }

  private def getLastChangedLimits(filters: Array[Filter]) = {
    val lastChangedLimits = filters.flatMap(getLastChangedLimit)

    Tuple2(
      // Note that this way of aggregating filters will not work with "Or" predicates.
      Try(lastChangedLimits.filter(_.isInstanceOf[Min]).max).toOption.map(_.value),
      Try(lastChangedLimits.filter(_.isInstanceOf[Max]).min).toOption.map(_.value)
    )
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val rdd = readRows(config.limit, getLastChangedLimits(filters))
    val newRdd = if (schema == defaultSchema || schema == null || schema.tail.isEmpty) {
      rdd
    } else {
      val jsonFieldsSchema = schemaWithoutRenamedColumns(StructType.apply(schema.tail.tail))
      val df = sqlContext.sparkSession.createDataFrame(rdd, defaultSchema)
      flattenAndRenameColumns(sqlContext, df, jsonFieldsSchema).rdd
    }

    newRdd.map(row => {
      val filteredCols = requiredColumns.map(colName => row.get(schema.fieldIndex(colName)))
      Row.fromSeq(filteredCols)
    })
  }

  override def insert(df: DataFrame, overwrite: scala.Boolean): scala.Unit = {
    if (!df.columns.contains("key")) {
      throw new IllegalArgumentException(
        "The dataframe used for insertion must have a \"key\" column.")
    }

    val (columnNames, dfWithUnRenamedKeyColumns) = prepareForInsert(df.drop(lastChangedColName))
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

    post(config, url, items)
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
  private val lastChangedColName = "lastChanged"
  private val keyColumnPattern = """^_*key$""".r
  private val lastChangedColumnPattern = """^_*lastChanged$""".r

  private def keyColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(keyColumnPattern.findFirstIn(_).isDefined)
  private def lastChangedColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(lastChangedColumnPattern.findFirstIn(_).isDefined)

  def rowsToRawItems(
      nonKeyColumnNames: Seq[String],
      rows: Seq[Row],
      mapper: ObjectMapper): Seq[RawItemForPost] =
    rows.map(
      row =>
        RawItemForPost(
          Option(row.getString(row.fieldIndex(temporaryKeyName)))
            .getOrElse(throw new IllegalArgumentException("\"key\" can not be null.")),
          io.circe.parser
            .decode[JsonObject](mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames)))
            .right
            .get
      ))

  private def schemaWithoutRenamedColumns(schema: StructType) =
    StructType.apply(schema.fields.map(field => {
      if (keyColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else if (lastChangedColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else {
        field
      }
    }))

  private def renameColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema) ++ lastChangedColumns(df.schema)
    // rename columns starting with the longest one first, to avoid creating a column with the same name
    columnsToRename.sorted.foldLeft(df) { (df, column) =>
      df.withColumnRenamed(column, s"_$column")
    }
  }

  private def unRenameColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema) ++ lastChangedColumns(df.schema)
    // when renaming them back we instead start with the shortest column name, for similar reasons
    columnsToRename.sortWith(_ > _).foldLeft(df) { (df, column) =>
      df.withColumnRenamed(column, column.substring(1))
    }
  }

  private val temporaryKeyName = s"TrE85tFQPCb2fEUZ"
  private val temporaryLastChangedName = s"J2p972xzM9bf32oD"

  def flattenAndRenameColumns(
      sqlContext: SQLContext,
      df: DataFrame,
      jsonFieldsSchema: StructType): DataFrame = {
    import org.apache.spark.sql.functions.from_json
    import sqlContext.implicits.StringToColumn
    import org.apache.spark.sql.functions.col
    val dfWithSchema =
      df.select(
        $"key",
        col(lastChangedColName),
        from_json($"columns", jsonFieldsSchema).alias("columns"))

    if (keyColumns(jsonFieldsSchema).isEmpty && lastChangedColumns(jsonFieldsSchema).isEmpty) {
      dfWithSchema.select("key", lastChangedColName, "columns.*")
    } else {
      val dfWithColumnsTmpRenamed = dfWithSchema
        .withColumnRenamed("key", temporaryKeyName)
        .withColumnRenamed(lastChangedColName, temporaryLastChangedName)
      val temporaryFlatDf = renameColumns(
        dfWithColumnsTmpRenamed.select(temporaryKeyName, temporaryLastChangedName, "columns.*"))
      temporaryFlatDf
        .withColumnRenamed(temporaryKeyName, "key")
        .withColumnRenamed(temporaryLastChangedName, lastChangedColName)
    }
  }

  def prepareForInsert(df: DataFrame): (Seq[String], DataFrame) = {
    val dfWithKeyRenamed = df
      .withColumnRenamed("key", temporaryKeyName)
      .withColumnRenamed(lastChangedColName, temporaryLastChangedName)
    val dfWithUnRenamedColumns = unRenameColumns(dfWithKeyRenamed)
    val columnNames = dfWithUnRenamedColumns.columns.filter(x =>
      !x.equals(temporaryKeyName) && !x.equals(temporaryLastChangedName))
    (columnNames, dfWithUnRenamedColumns)
  }
}
