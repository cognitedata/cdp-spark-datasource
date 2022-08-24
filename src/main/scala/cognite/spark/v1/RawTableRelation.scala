package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.getTimestampLimit
import com.cognite.sdk.scala.common.{CdpApiException, Items}
import com.cognite.sdk.scala.v1._
import fs2.Stream
import org.apache.spark.datasource.MetricsSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import java.time.Instant
import scala.util.Try

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
    with Serializable {
  import CdpConnector._
  import RawTableRelation._

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)

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
          Some(1),
          RawRowFilter(),
          Seq(),
          None,
          collectSchemaInferenceMetrics,
          false)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf =
        renameColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
      StructType(
        StructField("key", DataTypes.StringType, nullable = false)
          +: StructField(lastUpdatedTimeColName, DataTypes.TimestampType, nullable = true)
          +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  def getStreams(filter: RawRowFilter, cursors: Vector[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, RawRow]] = {
    assert(numPartitions == cursors.length)
    val rawClient = client.rawRows(database, table)
    cursors.map(rawClient.filterOnePartition(filter, _, limit))
  }

  def getStreamsByKeys(keys: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, RawRow]] = {
    val rawClient = client.rawRows(database, table)
    Seq(
      Stream
        .emits(keys)
        .parEvalMap[IO, Either[Throwable, RawRow]](numPartitions)(rawClient.retrieveByKey(_).attempt)
        .evalMapFilter {
          case Right(row) => IO(Some(row))
          case Left(err) =>
            err match {
              case CdpApiException(_, 404, _, _, _, _, _, _) =>
                IO(None)
              case _ =>
                IO.raiseError(err)
            }
        })
  }

  private def getRowConverter(schema: Option[StructType]): RawRow => Row =
    schema match {
      case Some(schema) =>
        // .tail.tail to skip the key and lastUpdatedTime columns, which are always the first two
        val jsonFieldsSchema = schemaWithoutRenamedColumns(StructType(schema.tail.tail))
        RawJsonConverter.makeRowConverter(
          schema,
          jsonFieldsSchema.fieldNames,
          lastUpdatedTimeColName,
          "key")
      case None =>
        RawJsonConverter.untypedRowConverter
    }

  private def readRows(
      limit: Option[Int],
      numPartitions: Option[Int],
      filter: RawRowFilter,
      requestedKeys: Seq[String],
      schema: Option[StructType],
      collectMetrics: Boolean = config.collectMetrics,
      collectTestMetrics: Boolean = config.collectTestMetrics): RDD[Row] = {
    val configWithLimit =
      config.copy(limitPerPartition = limit, partitions = numPartitions.getOrElse(config.partitions))

    @transient lazy val rowConverter = getRowConverter(schema)

    val streams: (GenericClient[IO], Option[Int], Int) => Seq[Stream[IO, RawRow]] = requestedKeys match {
      case Nil => {
        val partitionCursors =
          CdpConnector
            .clientFromConfig(config)
            .rawRows(database, table)
            .getPartitionCursors(filter, configWithLimit.partitions)
            .unsafeRunSync()
            .toVector
        getStreams(filter, partitionCursors)
      }
      case keys => getStreamsByKeys(keys.toArray)
    }

    SdkV1Rdd[RawRow, String](
      sqlContext.sparkContext,
      configWithLimit,
      (item: RawRow, partitionIndex: Option[Int]) => {
        if (collectMetrics) {
          rowsRead.inc()
        }
        if (collectTestMetrics) {
          @transient lazy val partitionSize =
            MetricsSource.getOrCreateCounter(
              config.metricsPrefix,
              s"raw.$database.$table.${partitionIndex.getOrElse(0)}.partitionSize")
          partitionSize.inc()
        }
        rowConverter(item)
      },
      (r: RawRow) => r.key,
      streams,
      deduplicateRows = true // if false we might end up with 429 when trying to update assets with multiple same request
    )
  }

  override def buildScan(): RDD[Row] = buildScan(schema.fieldNames, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (minLastUpdatedTime, maxLastUpdatedTime) = filtersToTimestampLimits(filters, "lastUpdatedTime")
    val filteredRequiredColumns = getJsonColumnNames(requiredColumns)
    val filteredSchemaFields = getJsonColumnNames(schema.fieldNames)
    val lengthOfRequiredColumnsAsString = requiredColumns.mkString(",").length

    // since filters act as an AND, only the first one including `key` constraints is important
    val requestedKeys: Array[String] =
      filters
        .collectFirst(filterToRequestedKeys)
        .getOrElse(Array.empty)

    val rawRowFilter =
      if (lengthOfRequiredColumnsAsString > 200 ||
        requiredColumns.contains("columns") ||
        requiredColumns.length == schema.length ||
        filteredRequiredColumns.length == filteredSchemaFields.length) {
        RawRowFilter(minLastUpdatedTime, maxLastUpdatedTime)
      } else {
        RawRowFilter(minLastUpdatedTime, maxLastUpdatedTime, Some(filteredRequiredColumns.toIndexedSeq))
      }

    val jsonSchema = if (schema == defaultSchema || schema == null || schema.tail.isEmpty) {
      None
    } else {
      Some(schema)
    }

    val rdd =
      readRows(config.limitPerPartition, None, rawRowFilter, requestedKeys, jsonSchema)

    rdd.map(row => {
      val filteredCols = requiredColumns.map(colName => row.get(schema.fieldIndex(colName)))
      new GenericRow(filteredCols)
    })
  }

  val filterToRequestedKeys: PartialFunction[Filter, Array[String]] = {
    case EqualTo("key", value) => Array(value.toString())
    case EqualNullSafe("key", value) => Array(value.toString())
    case In("key", values) => values.map(_.toString())
    case And(left, _) if filterToRequestedKeys.isDefinedAt(left) =>
      filterToRequestedKeys(left)
    case And(_, right) if filterToRequestedKeys.isDefinedAt(right) =>
      filterToRequestedKeys(right)
    case Or(left, right)
        if (filterToRequestedKeys.isDefinedAt(left) && filterToRequestedKeys.isDefinedAt(right)) =>
      (filterToRequestedKeys(left) ++ filterToRequestedKeys(right)).toSet.toArray
  }

  def filtersToTimestampLimits(
      filters: Array[Filter],
      colName: String): (Option[Instant], Option[Instant]) = {
    val timestampLimits = filters.flatMap(getTimestampLimit(_, colName))

    if (timestampLimits.exists(_.value.isBefore(Instant.ofEpochMilli(0)))) {
      throw new CdfSparkIllegalArgumentException("timestamp limits must exceed 1970-01-01T00:00:00Z")
    }

    Tuple2(
      // Note that this way of aggregating filters will not work with "Or" predicates.
      Try(timestampLimits.collect { case min: Min => min: Limit }.max).toOption
        .map(_.value),
      Try(timestampLimits.collect { case max: Max => max: Limit }.min).toOption
        .map(_.value)
    )
  }

  override def insert(df: DataFrame, overwrite: scala.Boolean): scala.Unit = {
    if (!df.columns.contains("key")) {
      throw new CdfSparkIllegalArgumentException(
        "The dataframe used for insertion must have a \"key\" column.")
    }

    val (columnNames, dfWithUnRenamedKeyColumns) = prepareForInsert(df.drop(lastUpdatedTimeColName))
    dfWithUnRenamedKeyColumns.foreachPartition((rows: Iterator[Row]) => {
      val batches = rows.grouped(batchSize).toVector
      batches
        .parTraverse_(postRows(columnNames, _))
        .unsafeRunSync()
    })
  }

  private def postRows(nonKeyColumnNames: Array[String], rows: Seq[Row]): IO[Unit] = {
    val items = RawJsonConverter.rowsToRawItems(nonKeyColumnNames, temporaryKeyName, rows)

    client
      .rawRows(database, table)
      .createItems(Items(items.map(_.toCreate).toVector), ensureParent = config.rawEnsureParent)
      .flatTap { _ =>
        IO {
          if (config.collectMetrics) {
            rowsCreated.inc(rows.length)
          }
        }
      }
  }
}

object RawTableRelation {
  private val lastUpdatedTimeColName = "lastUpdatedTime"
  private val keyColumnPattern = """^_*key$""".r
  private val lastUpdatedTimeColumnPattern = """^_*lastUpdatedTime$""".r

  val defaultSchema: StructType = StructType(
    Seq(
      StructField("key", DataTypes.StringType),
      StructField(lastUpdatedTimeColName, DataTypes.TimestampType),
      StructField("columns", DataTypes.StringType)
    ))

  private def keyColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(keyColumnPattern.findFirstIn(_).isDefined)
  private def lastUpdatedTimeColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(lastUpdatedTimeColumnPattern.findFirstIn(_).isDefined)

  private def unrenameColumn(col: String): Option[String] =
    if (keyColumnPattern.findFirstIn(col).isDefined) {
      Some(col.replaceFirst("_", ""))
    } else if (lastUpdatedTimeColumnPattern.findFirstIn(col).isDefined) {
      Some(col.replaceFirst("_", ""))
    } else {
      None
    }

  private def schemaWithoutRenamedColumns(schema: StructType) =
    StructType.apply(schema.fields.map(field => {
      unrenameColumn(field.name) match {
        case Some(newName) => field.copy(name = newName)
        case None => field
      }
    }))

  private def renameColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema) ++ lastUpdatedTimeColumns(df.schema)
    // rename columns starting with the longest one first, to avoid creating a column with the same name
    columnsToRename.sorted.foldLeft(df) { (df, column) =>
      df.withColumnRenamed(column, s"_$column")
    }
  }

  private def unRenameColumns(df: DataFrame): DataFrame = {
    val columnsToRename = keyColumns(df.schema) ++ lastUpdatedTimeColumns(df.schema)
    // when renaming them back we instead start with the shortest column name, for similar reasons
    columnsToRename.sortWith(_ > _).foldLeft(df) { (df, column) =>
      df.withColumnRenamed(column, column.substring(1))
    }
  }

  // unRename columns in the schema back to the raw json columns
  private def getJsonColumnNames(fieldNames: Array[String]): Array[String] =
    fieldNames.diff(Seq("key", lastUpdatedTimeColName)).map { n =>
      unrenameColumn(n).getOrElse(n)
    }

  private[cognite] val temporaryKeyName = s"TrE85tFQPCb2fEUZ"
  private[cognite] val temporaryLastUpdatedTimeName = s"J2p972xzM9bf32oD"

  def prepareForInsert(df: DataFrame): (Array[String], DataFrame) = {
    val dfWithKeyRenamed = df
      .withColumnRenamed("key", temporaryKeyName)
      .withColumnRenamed(lastUpdatedTimeColName, temporaryLastUpdatedTimeName)
    val dfWithUnRenamedColumns = unRenameColumns(dfWithKeyRenamed)
    val columnNames =
      dfWithUnRenamedColumns.columns.diff(Array(temporaryKeyName, temporaryLastUpdatedTimeName))
    (columnNames, dfWithUnRenamedColumns)
  }
}
