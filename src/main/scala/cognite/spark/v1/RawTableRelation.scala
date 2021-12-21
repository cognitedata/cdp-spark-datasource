package cognite.spark.v1

import java.time.Instant
import cats.effect.IO
import cats.implicits._
import cognite.spark.v1.PushdownUtilities.getTimestampLimit
import com.cognite.sdk.scala.common.Items
import com.cognite.sdk.scala.v1._
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.datasource.MetricsSource
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
    with Serializable {
  import RawTableRelation._
  import CdpConnector._

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

  def getStreams(filter: RawRowFilter)(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, RawRow]] =
    client.rawRows(database, table).filterPartitionsF(filter, numPartitions, limit).unsafeRunSync()

  private def readRows(
      limit: Option[Int],
      numPartitions: Option[Int],
      filter: RawRowFilter,
      collectMetrics: Boolean = config.collectMetrics,
      collectTestMetrics: Boolean = config.collectTestMetrics): RDD[Row] = {
    val configWithLimit =
      config.copy(limitPerPartition = limit, partitions = numPartitions.getOrElse(config.partitions))

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
    val columnsToIgnore = Seq("key", "lastUpdatedTime")
    val filteredRequiredColumns = requiredColumns.filterNot(columnsToIgnore.contains(_))
    val filteredSchemaFields = schema.fieldNames.filterNot(columnsToIgnore.contains(_))
    val lengthOfRequiredColumnsAsString = requiredColumns.mkString(",").length

    val rawRowFilter =
      if (filteredRequiredColumns.isEmpty ||
        lengthOfRequiredColumnsAsString > 200 ||
        requiredColumns.length == schema.length ||
        filteredRequiredColumns.length == filteredSchemaFields.length) {
        RawRowFilter(minLastUpdatedTime, maxLastUpdatedTime)
      } else {
        RawRowFilter(minLastUpdatedTime, maxLastUpdatedTime, Some(filteredRequiredColumns))
      }

    val rdd =
      readRows(config.limitPerPartition, None, rawRowFilter)
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
      throw new CdfSparkIllegalArgumentException("timestamp limits must exceed 1970-01-01T00:00:00Z")
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

  private def postRows(nonKeyColumnNames: Seq[String], rows: Seq[Row]): IO[Unit] = {
    val items = rowsToRawItems(nonKeyColumnNames, rows)

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

  def rowsToRawItems(nonKeyColumnNames: Seq[String], rows: Seq[Row]): Seq[RawRow] =
    rows.map(
      row =>
        RawRow(
          Option(row.getString(row.fieldIndex(temporaryKeyName)))
            .getOrElse(throw new CdfSparkIllegalArgumentException("\"key\" can not be null.")),
          nonKeyColumnNames.map(f => f -> anyToRawJson(row.getAs[Any](f))).toMap
      ))

  // scalastyle:off cyclomatic.complexity
  /** Maps the object from Spark into circe Json object. This is the types we need to cover (according to Row.get javadoc):
    * BooleanType -> java.lang.Boolean
       ByteType -> java.lang.Byte
       ShortType -> java.lang.Short
       IntegerType -> java.lang.Integer
       LongType -> java.lang.Long
       FloatType -> java.lang.Float
       DoubleType -> java.lang.Double
       StringType -> String
       DecimalType -> java.math.BigDecimal

       DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
       DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true

       TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
       TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true

       BinaryType -> byte array
       ArrayType -> scala.collection.Seq (use getList for java.util.List)
       MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
       StructType -> org.apache.spark.sql.Row */
  private def anyToRawJson(v: Any): Json =
    v match {
      case v: java.lang.Boolean => Json.fromBoolean(v.booleanValue)
      case v: java.lang.Float => Json.fromFloatOrString(v.floatValue)
      case v: java.lang.Double => Json.fromDoubleOrString(v.doubleValue)
      case v: java.math.BigDecimal => Json.fromBigDecimal(v)
      case v: java.lang.Number => Json.fromLong(v.longValue())
      case v: String => Json.fromString(v)
      case v: java.sql.Date => Json.fromString(v.toString)
      case v: java.time.LocalDate => Json.fromString(v.toString)
      case v: java.sql.Timestamp => Json.fromString(v.toString)
      case v: java.time.Instant => Json.fromString(v.toString)
      case v: Array[Byte] =>
        throw new CdfSparkIllegalArgumentException(
          "BinaryType is not supported when writing raw, please convert it to base64 string or array of numbers")
      case v: Seq[Any] => Json.arr(v.map(anyToRawJson): _*)
      case v: Map[Any @unchecked, Any @unchecked] =>
        Json.obj(v.toSeq.map(x => x._1.toString -> anyToRawJson(x._2)): _*)
      case v: Row => rowToJson(v)
    }

  private def rowToJson(r: Row): Json =
    if (r.schema != null) {
      Json.obj(r.schema.fieldNames.map(f => f -> anyToRawJson(r.getAs[Any](f))): _*)
    } else {
      Json.arr(r.toSeq.map(anyToRawJson): _*)
    }

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
