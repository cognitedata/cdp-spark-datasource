package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.getTimestampLimit
import com.codahale.metrics.Counter
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

  private val MaxKeysAllowedForFiltering = 10000L

  @transient lazy val client: GenericClient[IO] =
    CdpConnector.clientFromConfig(config)

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)

  // TODO: check if we need to sanitize the database and table names, or if they are reasonably named
  private def rowsCreated: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"raw.$database.$table.rows.read")
  private def rowsRead: Counter =
    MetricsSource.getOrCreateCounter(config.metricsPrefix, s"raw.$database.$table.rows.created")

  override val schema: StructType = userSchema.getOrElse {
    if (inferSchema) {
      val rdd =
        readRows(
          limit = inferSchemaLimit.orElse(Some(Constants.DefaultInferSchemaLimit)),
          numPartitions = Some(1),
          filter = RawRowFilter(),
          requestedKeys = None,
          schema = None,
          collectMetrics = collectSchemaInferenceMetrics,
          collectTestMetrics = false
        )

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

  private def getStreams(filter: RawRowFilter, cursors: Vector[String])(
      limit: Option[Int],
      numPartitions: Int)(client: GenericClient[IO]): Seq[Stream[IO, RawRow]] = {
    assert(numPartitions == cursors.length)
    val rawClient = client.rawRows(database, table)
    cursors.map(rawClient.filterOnePartition(filter, _, limit))
  }

  private def getStreamByKeys(client: GenericClient[IO], keys: Set[String]): Stream[IO, RawRow] = {
    val rawClient = client.rawRows(database, table)
    Stream
      .emits(keys.toSeq)
      .parEvalMapUnbounded(
        rawClient
          .retrieveByKey(_)
          .map(Option(_))
          .recover {
            // It's fine to request keys that don't exist, so we just ignore 404 responses.
            case CdpApiException(_, 404, _, _, _, _, _, _) => None
          }
      )
      .collect { case Some(v) => v }
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

  // scalastyle:off method.length
  private def readRows(
      limit: Option[Int],
      numPartitions: Option[Int],
      filter: RawRowFilter,
      requestedKeys: Option[Set[String]],
      schema: Option[StructType],
      collectMetrics: Boolean = config.collectMetrics,
      collectTestMetrics: Boolean = config.collectTestMetrics): RDD[Row] = {
    val configWithLimit =
      config.copy(limitPerPartition = limit, partitions = numPartitions.getOrElse(config.partitions))
    @transient lazy val rowConverter = getRowConverter(schema)
    val streams: GenericClient[IO] => Seq[Stream[IO, RawRow]] = {
      requestedKeys match {
        // Request individual keys from CDF, unless there are too many, as each key requires a request.
        case Some(keys) if keys.size < MaxKeysAllowedForFiltering =>
          client: GenericClient[IO] =>
            Seq(getStreamByKeys(client, keys))
        case _ =>
          val partitionCursors =
            CdpConnector
              .clientFromConfig(config)
              .rawRows(database, table)
              .getPartitionCursors(filter, configWithLimit.partitions)
              .unsafeRunSync()
              .toVector
          getStreams(filter, partitionCursors)(
            configWithLimit.limitPerPartition,
            configWithLimit.partitions)
      }
    }

    SdkV1Rdd[RawRow, String](
      sqlContext.sparkContext,
      configWithLimit,
      (item: RawRow, partitionIndex: Option[Int]) => {
        if (collectMetrics) {
          rowsRead.inc()
        }
        if (collectTestMetrics) {
          MetricsSource
            .getOrCreateCounter(
              config.metricsPrefix,
              s"raw.$database.$table.${partitionIndex.getOrElse(0)}.partitionSize")
            .inc()
        }
        rowConverter(item)
      },
      (r: RawRow) => r.key,
      streams,
      deduplicateRows = true // if false we might end up with 429 when trying to update assets with multiple same request
    )
  }
  // scalastyle:on method.length

  override def buildScan(): RDD[Row] = buildScan(schema.fieldNames, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (minLastUpdatedTime, maxLastUpdatedTime) = filtersToTimestampLimits(filters, "lastUpdatedTime")
    val requestedKeys: Option[Set[String]] = filtersToRequestedKeys(filters)
    val filteredRequiredColumns = getJsonColumnNames(requiredColumns)
    val filteredSchemaFields = getJsonColumnNames(schema.fieldNames)
    val lengthOfRequiredColumnsAsString = requiredColumns.mkString(",").length

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

  private def filterOr(or: Or): Option[Set[String]] =
    (filterToRequestedKeys(or.left), filterToRequestedKeys(or.right)) match {
      case (Some(leftRequestedKeys), Some(rightRequestedKeys)) =>
        Some(leftRequestedKeys.union(rightRequestedKeys))
      case _ => None
    }

  private def filterAnd(and: And): Option[Set[String]] =
    (filterToRequestedKeys(and.left), filterToRequestedKeys(and.right)) match {
      case (Some(leftRequestedKeys), Some(rightRequestedKeys)) =>
        Some(leftRequestedKeys.intersect(rightRequestedKeys))
      case (None, someRightRequestedKeys) => someRightRequestedKeys
      case (someLeftRequestedKeys, None) => someLeftRequestedKeys
    }

  def filterToRequestedKeys(filter: Filter): Option[Set[String]] =
    filter match {
      case EqualTo("key", value) => Some(Set(value.toString))
      case EqualNullSafe("key", value) => Some(Set(value.toString))
      case In("key", values) => Some(values.map(_.toString).toSet)
      case IsNull("key") => Some(Set.empty)
      case or: Or => filterOr(or)
      case and: And => filterAnd(and)
      case _ => None
    }

  // A return value of None means no specific keys were requested.
  // Some(Set.empty) would mean that the filters result in requesting zero keys,
  // for example if filtering with "WHERE key = 'k1' AND key = 'k2'.
  def filtersToRequestedKeys(filters: Array[Filter]): Option[Set[String]] =
    filters
      .map(filterToRequestedKeys)
      .foldLeft(Option.empty[Set[String]]) {
        case (None, maybeMoreKeys) => maybeMoreKeys
        case (result, None) => result
        case (Some(result), Some(moreKeys)) => Some(result.intersect(moreKeys))
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
            rowsCreated.inc(rows.length.toLong)
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
