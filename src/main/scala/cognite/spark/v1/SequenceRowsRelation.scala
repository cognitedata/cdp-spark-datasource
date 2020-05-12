package cognite.spark.v1

import cats.effect.IO
import cats.implicits._
import com.cognite.sdk.scala.common.CdpApiException
import com.cognite.sdk.scala.v1._
import fs2.Stream
import io.circe.Json
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class SequenceRowsRelation(config: RelationConfig, sequenceId: CogniteId)(val sqlContext: SQLContext)
    extends CdfRelation(config, "sequencerows")
    with WritableRelation
    with PrunedFilteredScan {
  import SequenceRowsRelation._

  @transient lazy private val batchSize = config.batchSize.getOrElse(Constants.DefaultRawBatchSize)

  val sequenceInfo: Sequence = (sequenceId match {
    case CogniteExternalId(externalId) => client.sequences.retrieveByExternalId(externalId)
    case CogniteInternalId(id) => client.sequences.retrieveById(id)
  }).handleError {
      case e: CdpApiException =>
        throw new Exception(s"Could not resolve schema of sequence $sequenceId.", e)
    }
    .unsafeRunSync()
  val columnTypes: Map[String, String] =
    sequenceInfo.columns.map(c => c.externalId -> c.valueType).toList.toMap

  override val schema: StructType = new StructType(
    Array(StructField("rowNumber", DataTypes.LongType, nullable = false)) ++ sequenceInfo.columns
      .map(col => StructField(col.externalId, sequenceTypeToSparkType(col.valueType)))
      .toList
  )

  private def query(
      filter: SequenceRowFilter,
      limit: Option[Int],
      columns: Option[Seq[String]],
      client: GenericClient[IO, Nothing]) =
    sequenceId match {
      case CogniteExternalId(externalId) =>
        client.sequenceRows.queryByExternalId(
          externalId,
          filter.inclusiveStart,
          filter.exclusiveEnd,
          limit,
          columns)
      case CogniteInternalId(id) =>
        client.sequenceRows.queryById(id, filter.inclusiveStart, filter.exclusiveEnd, limit, columns)
    }

  private def insert(columns: Seq[String], rows: Seq[SequenceRow], client: GenericClient[IO, Nothing]) =
    sequenceId match {
      case CogniteExternalId(externalId) =>
        client.sequenceRows.insertByExternalId(externalId, columns, rows)
      case CogniteInternalId(id) =>
        client.sequenceRows.insertById(id, columns, rows)
    }

  def getStreams(filters: Seq[SequenceRowFilter], expectedColumns: Array[String])(
      client: GenericClient[IO, Nothing],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedSequenceRow]] =
    filters.toVector.map { filter =>
      val requestedColumns =
        if (expectedColumns.isEmpty) {
          // when no columns are needed, the API does not like it, so we have to request something
          // prefer non-string columns since they can't contain too much data
          Array(
            sequenceInfo.columns
              .find(_.valueType != "STRING")
              .getOrElse(sequenceInfo.columns.head)
              .externalId
          )
        } else {
          expectedColumns
        }
      val projectedRows =
        query(filter, limit, Some(requestedColumns), client)
          .map {
            case (_, rows) =>
              rows.map { r =>
                val values = expectedColumns
                  .zip(r.values)
                  .map {
                    case (column, value) =>
                      parseJsonValue(value, columnTypes(column))
                        .getOrElse(throw new Exception(s"Unexpected value $value in column $column"))
                  }
                ProjectedSequenceRow(r.rowNumber, values)
              }
          }

      fs2.Stream.eval(projectedRows).flatMap(r => r)
    }

  def fromRow(schema: StructType): (Array[String], Row => SequenceRow) = {
    val rowNumberIndex = schema.fieldNames.indexOf("rowNumber")
    if (rowNumberIndex < 0) {
      throw new Exception("Can't upsert sequence rows, column `rowNumber` is missing.")
    }

    val columns = schema.fields.zipWithIndex.filter(_._1.name != "rowNumber").map {
      case (field, index) =>
        val seqColumn = sequenceInfo.columns
          .find(_.externalId == field.name)
          .getOrElse(throw new Exception(
            s"Can't insert column `${field.name}` into sequence $sequenceId, the column does not exist in the sequence definition"))
        def jsonFromDouble(num: Double): Json =
          Json.fromDouble(num).getOrElse(throw new Exception(s"Numeric value $num"))
        val tryGetValue: PartialFunction[Any, Json] = seqColumn.valueType match {
          case "DOUBLE" => {
            case null => Json.Null
            case x: Double => jsonFromDouble(x)
            case x: Int => jsonFromDouble(x.toDouble)
            case x: Float => jsonFromDouble(x.toDouble)
            case x: Long => jsonFromDouble(x.toDouble)
            case x: java.math.BigDecimal => jsonFromDouble(x.doubleValue)
            case x: java.math.BigInteger => jsonFromDouble(x.doubleValue)
          }
          case "LONG" => {
            case null => Json.Null
            case x: Int => Json.fromInt(x)
            case x: Long => Json.fromLong(x)
          }
          case "STRING" => {
            case null => Json.Null
            case x: String => Json.fromString(x)
          }
        }
        (index, seqColumn, tryGetValue)
    }

    def parseRow(row: Row): SequenceRow = {
      val rowNumber = row.get(rowNumberIndex) match {
        case x: Long => x
        case x: Int => x: Long
        case _ => throw SparkSchemaHelperRuntime.badRowError(row, "rowNumber", "Long", "")
      }
      val columnValues = columns.map {
        case (index, seqColumn, tryGetValue) =>
          tryGetValue.applyOrElse(
            row.get(index),
            (_: Any) =>
              throw SparkSchemaHelperRuntime
                .badRowError(row, seqColumn.externalId, seqColumn.valueType, "")
          )
      }
      SequenceRow(rowNumber, columnValues)
    }

    (columns.map(_._2.externalId), parseRow)
  }

  def delete(rows: Seq[Row]): IO[Unit] = ???
  def insert(rows: Seq[Row]): IO[Unit] =
    throw new Exception("Insert not supported for sequenceRows. Use upsert instead.")
  def update(rows: Seq[Row]): IO[Unit] =
    throw new Exception("Update not supported for sequenceRows. Use upsert instead.")
  def upsert(rows: Seq[Row]): IO[Unit] = {
    if (rows.isEmpty) return IO.unit

    val (columns, fromRowFn) = fromRow(rows.head.schema)
    val projectedRows = rows.map(fromRowFn)

    val batches = projectedRows.grouped(Constants.DefaultSequencesLimit).toVector
    batches
      .traverse_(batch => insert(columns, batch, client))
  }

  private def readRows(
      limit: Option[Int],
      filters: Seq[SequenceRowFilter],
      columns: Array[String]): RDD[Row] = {
    val configWithLimit =
      config.copy(limitPerPartition = limit)
    val rowNumberIndex = columns.indexOf("rowNumber")

    SdkV1Rdd[ProjectedSequenceRow, Long](
      sqlContext.sparkContext,
      configWithLimit,
      (item: ProjectedSequenceRow) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        Row.fromSeq(if (rowNumberIndex < 0) {
          item.values
        } else {
          val (beforeRowNumber, afterRowNumber) = item.values.splitAt(rowNumberIndex)
          beforeRowNumber ++ Array(item.rowNumber) ++ afterRowNumber
        })
      },
      (r: ProjectedSequenceRow) => r.rowNumber,
      getStreams(filters, columns.filter(_ != "rowNumber"))
    )
  }

//  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val filterObjects =
      filters
        .map(getSeqFilter)
        .reduceOption(filterIntersection)
        .getOrElse(Seq(SequenceRowFilter()))

    val rdd =
      readRows(
        config.limitPerPartition,
        filterObjects,
        requiredColumns
      )
    rdd
  }
}

object SequenceRowsRelation {

  private def parseValue(value: Any, offset: Long = 0) = Some(value.asInstanceOf[Long] + offset)
  def getSeqFilter(filter: Filter): Seq[SequenceRowFilter] =
    filter match {
      case EqualTo("rowNumber", value) =>
        Seq(SequenceRowFilter(parseValue(value), parseValue(value, +1)))
      case EqualNullSafe("rowNumber", value) =>
        Seq(SequenceRowFilter(parseValue(value), parseValue(value, +1)))
      case In("rowNumber", values) =>
        values
          .filter(_ != null)
          .map(value => SequenceRowFilter(parseValue(value), parseValue(value, +1)))
      case LessThan("rowNumber", value) => Seq(SequenceRowFilter(exclusiveEnd = parseValue(value)))
      case LessThanOrEqual("rowNumber", value) =>
        Seq(SequenceRowFilter(exclusiveEnd = parseValue(value, +1)))
      case GreaterThan("rowNumber", value) =>
        Seq(SequenceRowFilter(inclusiveStart = parseValue(value, +1)))
      case GreaterThanOrEqual("rowNumber", value) =>
        Seq(SequenceRowFilter(inclusiveStart = parseValue(value)))
      case And(f1, f2) => filterIntersection(getSeqFilter(f1), getSeqFilter(f2))
      case Or(f1, f2) => getSeqFilter(f1) ++ getSeqFilter(f2)
      case Not(f) => filterComplement(getSeqFilter(f))
      case _ => Seq(SequenceRowFilter())
    }

  final case class IntervalBorder(start: Boolean, value: Long)
  private def toBorders(f: Vector[SequenceRowFilter]) = {
    val borders = f
      .flatMap(
        f =>
          f.inclusiveStart.map(IntervalBorder(true, _)).toSeq ++ f.exclusiveEnd.map(
            IntervalBorder(false, _))
      )
      .sortBy(_.value)
    val plusInfCount = f.count(_.exclusiveEnd.isEmpty)
    val minusInfCount = f.count(_.inclusiveStart.isEmpty)
    (minusInfCount, plusInfCount, borders)
  }

  private def toSegments(f: Vector[SequenceRowFilter]) = {
    val (minusInfCount, plusInfCount, borders) = toBorders(f.toVector)
    // count number of overlapping intervals in each segment
    val segmentValues = borders.scanLeft[Int, Vector[Int]](minusInfCount)((v, b) => {
      if (b.start) {
        v + 1
      } else {
        v - 1
      }
    })
    val borderLabels = Vector(None) ++ borders.map(b => Some(b.value)) ++ Vector(None)
    val segmentLabels =
      borderLabels
        .zip(borderLabels.drop(1))
        .map { case (low, high) => SequenceRowFilter(low, high) }
    segmentLabels.zip(segmentValues)
  }

  def normalizeFilterSet(f: Seq[SequenceRowFilter]): Vector[SequenceRowFilter] =
    toSegments(f.toVector)
      .collect { case (filter, count) if count >= 1 => filter }

  def filterIntersection(
      a: Seq[SequenceRowFilter],
      b: Seq[SequenceRowFilter]): Vector[SequenceRowFilter] =
    toSegments(normalizeFilterSet(a) ++ normalizeFilterSet(b))
      .collect { case (filter, count) if count >= 2 => filter }

  def filterComplement(a: Seq[SequenceRowFilter]): Vector[SequenceRowFilter] =
    toSegments(a.toVector)
      .collect { case (filter, count) if count == 2 => filter }

  def parseJsonValue(v: Json, columnType: String): Option[Any] =
    if (v.isNull) {
      Some(null)
    } else {
      columnType match {
        case "STRING" => v.asString
        case "DOUBLE" => v.asNumber.map(_.toDouble)
        case "LONG" => v.asNumber.flatMap(_.toLong)
        case a => throw new Exception(s"Unknown column type $a")
      }
    }

  def sequenceTypeToSparkType(columnType: String): DataType =
    columnType match {
      case "STRING" => DataTypes.StringType
      case "DOUBLE" => DataTypes.DoubleType
      case "LONG" => DataTypes.LongType
      case a => throw new Exception(s"Unknown column type $a")
    }
}

final case class ProjectedSequenceRow(rowNumber: Long, values: Array[Any])
final case class SequenceRowFilter(
    inclusiveStart: Option[Long] = None,
    exclusiveEnd: Option[Long] = None)
