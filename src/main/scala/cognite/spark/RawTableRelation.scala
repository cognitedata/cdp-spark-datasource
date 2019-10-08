package cognite.spark

import java.time.Instant

import cats.effect.{ContextShift, IO}
import cats.implicits._
import cognite.spark.PushdownUtilities.getTimestampLimit
import com.cognite.sdk.scala.v1.{GenericClient, RawRow, RawRowFilter}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.circe.{Json, JsonObject}
import io.circe.syntax._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.datasource.MetricsSource

import scala.concurrent.ExecutionContext
import com.cognite.sdk.scala.common.Auth
import fs2.Stream

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
    with CdpConnector
    with Serializable {
  import RawTableRelation._

  import CdpConnector.sttpBackend
  implicit val auth: Auth = config.auth
  @transient lazy val client = new GenericClient[IO, Nothing](Constants.SparkDatasourceVersion)

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)

  @transient lazy val defaultSchema = StructType(
    Seq(
      StructField("key", DataTypes.StringType),
      StructField(lastUpdatedTimeColName, DataTypes.TimestampType),
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
          RawRowFilter(),
          collectSchemaInferenceMetrics)

      import sqlContext.sparkSession.implicits._
      val df = sqlContext.createDataFrame(rdd, defaultSchema)
      val jsonDf =
        renameColumns(sqlContext.sparkSession.read.json(df.select($"columns").as[String]))
      StructType(
        StructField("key", DataTypes.StringType, false)
          +: StructField(lastUpdatedTimeColName, DataTypes.TimestampType, true)
          +: jsonDf.schema.fields)
    } else {
      defaultSchema
    }
  }

  def getStreams(filter: RawRowFilter)(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, RawRow]] =
    client.rawRows(database, table).filterPartitionsF(filter, numPartitions, limit).unsafeRunSync()

  private def readRows(
      limit: Option[Int],
      filter: RawRowFilter,
      collectMetrics: Boolean = config.collectMetrics): RDD[Row] = {
    val configWithLimit = config.copy(limit = limit)

    SdkV1Rdd[RawRow, String](
      sqlContext.sparkContext,
      configWithLimit,
      (item: RawRow) => {
        if (collectMetrics) {
          rowsRead.inc()
        }
        Row(
          item.key,
          item.lastUpdatedTime.map(java.sql.Timestamp.from).orNull,
          JsonObject(item.columns.toSeq: _*).asJson.noSpaces)
      },
      (r: RawRow) => r.key,
      getStreams(filter)
    )
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val (minLastUpdatedTime, maxLastUpdatedTime) = filtersToTimestampLimits(filters, "lastUpdatedTime")

    val rdd = readRows(config.limit, RawRowFilter(minLastUpdatedTime, maxLastUpdatedTime))
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

  def filtersToTimestampLimits(
      filters: Array[Filter],
      colName: String): (Option[Instant], Option[Instant]) = {
    val timestampLimits = filters.flatMap(getTimestampLimit(_, colName))

    if (timestampLimits.exists(_.value.isBefore(Instant.ofEpochMilli(0)))) {
      sys.error("timestamp limits must exceed 1970-01-01T00:00:00Z")
    }

    Tuple2(
      // Note that this way of aggregating filters will not work with "Or" predicates.
      Try(timestampLimits.filter(_.isInstanceOf[Min]).max).toOption
        .map(_.value),
      Try(timestampLimits.filter(_.isInstanceOf[Max]).min).toOption
        .map(_.value)
    )
  }

  override def insert(df: DataFrame, overwrite: scala.Boolean): scala.Unit = {
    if (!df.columns.contains("key")) {
      throw new IllegalArgumentException("The dataframe used for insertion must have a \"key\" column.")
    }

    val (columnNames, dfWithUnRenamedKeyColumns) = prepareForInsert(df.drop(lastUpdatedTimeColName))
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

    client.rawRows(database, table).createFromRead(items).flatTap { _ =>
      IO {
        if (config.collectMetrics) {
          rowsCreated.inc(rows.length)
        }
      }
    } *> IO.unit
  }
}

object RawTableRelation {
  private val lastUpdatedTimeColName = "lastUpdatedTime"
  private val keyColumnPattern = """^_*key$""".r
  private val lastUpdatedTimeColumnPattern = """^_*lastUpdatedTime$""".r

  private def keyColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(keyColumnPattern.findFirstIn(_).isDefined)
  private def lastUpdatedTimeColumns(schema: StructType): Array[String] =
    schema.fieldNames.filter(lastUpdatedTimeColumnPattern.findFirstIn(_).isDefined)

  def rowsToRawItems(nonKeyColumnNames: Seq[String], rows: Seq[Row], mapper: ObjectMapper): Seq[RawRow] =
    rows.map(
      row =>
        RawRow(
          Option(row.getString(row.fieldIndex(temporaryKeyName)))
            .getOrElse(throw new IllegalArgumentException("\"key\" can not be null.")),
          io.circe.parser
            .decode[Map[String, Json]](
              mapper.writeValueAsString(row.getValuesMap[Any](nonKeyColumnNames)))
            .right
            .get
      ))

  private def schemaWithoutRenamedColumns(schema: StructType) =
    StructType.apply(schema.fields.map(field => {
      if (keyColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else if (lastUpdatedTimeColumnPattern.findFirstIn(field.name).isDefined) {
        field.copy(name = field.name.replaceFirst("_", ""))
      } else {
        field
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

  private val temporaryKeyName = s"TrE85tFQPCb2fEUZ"
  private val temporarylastUpdatedTimeName = s"J2p972xzM9bf32oD"

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
        col(lastUpdatedTimeColName),
        from_json($"columns", jsonFieldsSchema).alias("columns"))

    if (keyColumns(jsonFieldsSchema).isEmpty && lastUpdatedTimeColumns(jsonFieldsSchema).isEmpty) {
      dfWithSchema.select("key", lastUpdatedTimeColName, "columns.*")
    } else {
      val dfWithColumnsTmpRenamed = dfWithSchema
        .withColumnRenamed("key", temporaryKeyName)
        .withColumnRenamed(lastUpdatedTimeColName, temporarylastUpdatedTimeName)
      val temporaryFlatDf = renameColumns(
        dfWithColumnsTmpRenamed.select(temporaryKeyName, temporarylastUpdatedTimeName, "columns.*"))
      temporaryFlatDf
        .withColumnRenamed(temporaryKeyName, "key")
        .withColumnRenamed(temporarylastUpdatedTimeName, lastUpdatedTimeColName)
    }
  }

  def prepareForInsert(df: DataFrame): (Seq[String], DataFrame) = {
    val dfWithKeyRenamed = df
      .withColumnRenamed("key", temporaryKeyName)
      .withColumnRenamed(lastUpdatedTimeColName, temporarylastUpdatedTimeName)
    val dfWithUnRenamedColumns = unRenameColumns(dfWithKeyRenamed)
    val columnNames =
      dfWithUnRenamedColumns.columns.filter(x =>
        !x.equals(temporaryKeyName) && !x.equals(temporarylastUpdatedTimeName))
    (columnNames, dfWithUnRenamedColumns)
  }
}
