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

import scala.util.Try

case class SequenceRowWithId(id: CogniteId, sequenceRow: SequenceRow)

class SequenceRowsRelation(config: RelationConfig, sequenceId: CogniteId)(val sqlContext: SQLContext)
    extends CdfRelation(config, "sequencerows")
    with WritableRelation
    with PrunedFilteredScan {
  import SequenceRowsRelation._
  import CdpConnector._

  val sequenceInfo: Sequence = (sequenceId match {
    case CogniteExternalId(externalId) => client.sequences.retrieveByExternalId(externalId)
    case CogniteInternalId(id) => client.sequences.retrieveById(id)
  }).adaptError {
      case e: CdpApiException =>
        new CdfSparkException(s"Could not resolve schema of sequence $sequenceId.", e)
    }
    .unsafeRunSync()

  private val columnsWithoutExternalId =
    sequenceInfo.columns.zipWithIndex
      .filter { case (column, _) => column.externalId.isEmpty }

  if (columnsWithoutExternalId.nonEmpty) {
    val formattedId = sequenceId match {
      case CogniteExternalId(externalId) => s"with externalId '$externalId'"
      case CogniteInternalId(id) => s"with id $id"
    }
    val commaSeparatedInvalidColumns =
      columnsWithoutExternalId
        .map {
          case (column, index) =>
            Seq(
              Some(s"index=$index"),
              Some(s"type=${column.valueType}"),
              column.name.map(x => s"name=$x"),
              column.description.map(x => s"description=$x")
            ).flatten.mkString("(", ", ", ")")
        }
        .mkString(", ")

    throw new CdfSparkException(
      s"Sequence $formattedId contains columns without an externalId. " +
        "This is no longer supported, and is now required when creating new sequences. " +
        s"Invalid columns: [$commaSeparatedInvalidColumns]")
  }

  val columnTypes: Map[String, String] =
    sequenceInfo.columns.map(c => c.externalId.get -> c.valueType).toList.toMap

  override val schema: StructType = new StructType(
    Array(StructField("rowNumber", DataTypes.LongType, nullable = false)) ++ sequenceInfo.columns
      .map(col => StructField(col.externalId.get, sequenceTypeToSparkType(col.valueType)))
      .toList
  )

  def getStreams(filters: Seq[SequenceRowFilter], expectedColumns: Array[String])(
      client: GenericClient[IO],
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
              .get
          )
        } else {
          expectedColumns
        }
      val projectedRows =
        client.sequenceRows
          .queryById(
            sequenceInfo.id,
            filter.inclusiveStart,
            filter.exclusiveEnd,
            limit,
            Some(requestedColumns))
          .map {
            case (_, rows) =>
              rows.map { r =>
                val values = expectedColumns
                  .zip(r.values)
                  .map {
                    case (column, value) =>
                      parseJsonValue(value, columnTypes(column))
                        .getOrElse(
                          throw new CdfSparkException(s"Unexpected value $value in column $column"))
                  }
                ProjectedSequenceRow(r.rowNumber, values)
              }
          }

      fs2.Stream.eval(projectedRows).flatMap(r => r)
    }

  private def jsonFromDouble(num: Double): Json =
    Json.fromDouble(num).getOrElse(throw new CdfSparkException(s"Numeric value $num"))
  private def tryGetValue(columnType: String): PartialFunction[Any, Json] = // scalastyle:off
    columnType match {
      case "DOUBLE" => {
        case null => Json.Null // scalastyle:off null
        case x: Double => jsonFromDouble(x)
        case x: Int => jsonFromDouble(x.toDouble)
        case x: Float => jsonFromDouble(x.toDouble)
        case x: Long => jsonFromDouble(x.toDouble)
        case x: java.math.BigDecimal => jsonFromDouble(x.doubleValue)
        case x: java.math.BigInteger => jsonFromDouble(x.doubleValue)
      }
      case "LONG" => {
        case null => Json.Null // scalastyle:off null
        case x: Int => Json.fromInt(x)
        case x: Long => Json.fromLong(x)
      }
      case "STRING" => {
        case null => Json.Null // scalastyle:off null
        case x: String => Json.fromString(x)
      }
    }

  def fromRow(schema: StructType): (Array[String], Row => SequenceRowWithId) = {
    val rowNumberIndex = schema.fieldNames.indexOf("rowNumber")
    if (rowNumberIndex < 0) {
      throw new CdfSparkException("Can't upsert sequence rows, column `rowNumber` is missing.")
    }
    val externalIdIndex = schema.fieldNames.indexOf("externalId")
    val idIndex = schema.fieldNames.indexOf("id")
    if (externalIdIndex < 0 && idIndex < 0) {
      throw new CdfSparkException(
        "Can't upsert sequence rows, at least column `externalId` or `id` must be provided.")
    }

    val columns = schema.fields.zipWithIndex
      .filter(cols => !Seq("rowNumber", "externalId", "id").contains(cols._1.name))
      .map {
        case (field, index) =>
          val columnType = columnTypes.getOrElse(
            field.name,
            throw new CdfSparkException(
              s"Can't insert column `${field.name}` into sequence $sequenceId, the column does not exist in the sequence definition")
          )
          (index, field.name, columnType)
      }

    def parseRow(row: Row): SequenceRowWithId = {
      val rowNumber = row.get(rowNumberIndex) match {
        case x: Long => x
        case x: Int => x: Long
        case _ => throw SparkSchemaHelperRuntime.badRowError(row, "rowNumber", "Long", "")
      }

      val maybeExternalId = if (externalIdIndex >= 0) {
        row.get(externalIdIndex) match {
          case x: String => Some(x)
          case _ => None
        }
      } else None

      val maybeInternalId = if (idIndex >= 0) {
        row.get(idIndex) match {
          case x: Long => Some(x)
          case x: Int => Some(x.toLong)
          case _ => None
        }
      } else None

      val id = (maybeInternalId, maybeExternalId) match {
        case (Some(internalId), _) => CogniteInternalId(internalId)
        case (None, Some(externalId)) => CogniteExternalId(externalId)
        case (None, None) =>
          throw new CdfSparkException(
            "Can't upsert sequence rows, at least `id` or `externalId` must be provided.")
      }

      val columnValues = columns.map {
        case (index, name, columnType) =>
          tryGetValue(columnType).applyOrElse(
            row.get(index),
            (_: Any) => throw SparkSchemaHelperRuntime.badRowError(row, name, columnType, ""))
      }
      SequenceRowWithId(id, SequenceRow(rowNumber, columnValues))
    }

    (columns.map(_._2), parseRow)
  }

  def delete(rows: Seq[Row]): IO[Unit] = {
    val deletes = rows.map(r => SparkSchemaHelper.fromRow[SequenceRowDeleteSchema](r))
    client.sequenceRows
      .deleteById(sequenceInfo.id, deletes.map(_.rowNumber))
      .flatTap(_ => incMetrics(itemsDeleted, rows.length))
  }
  def insert(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Insert not supported for sequencerows. Use upsert instead.")

  def update(rows: Seq[Row]): IO[Unit] =
    throw new CdfSparkException("Update not supported for sequencerows. Use upsert instead.")

  def upsert(rows: Seq[Row]): IO[Unit] =
    if (rows.isEmpty) {
      IO.unit
    } else {
      val (columns, fromRowFn) = fromRow(rows.head.schema)
      val projectedRows = rows.map(fromRowFn)

      import cats.instances.list._
      projectedRows
        .groupBy(_.id)
        .toList
        .parTraverse {
          case (cogniteExternalId, rows) =>
            cogniteExternalId match {
              case externalId: CogniteExternalId =>
                client.sequenceRows
                  .insertByExternalId(externalId.externalId, columns, rows.map(_.sequenceRow))
                  .flatTap(_ => incMetrics(itemsCreated, rows.length))
              case internalId: CogniteInternalId =>
                client.sequenceRows
                  .insertById(internalId.id, columns, rows.map(_.sequenceRow))
                  .flatTap(_ => incMetrics(itemsCreated, rows.length))
            }

        } *> IO.unit
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
      (item: ProjectedSequenceRow, None) => {
        if (config.collectMetrics) {
          itemsRead.inc()
        }
        Row.fromSeq(if (rowNumberIndex < 0) {
          // when the rowNumber column is not expected
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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val filterObjects =
      filters
        .map(getSeqFilter)
        .reduceOption(filterIntersection)
        .getOrElse(Seq(SequenceRowFilter()))

    readRows(
      config.limitPerPartition,
      filterObjects,
      requiredColumns
    )
  }
}

object SequenceRowsRelation {

  private def parseValue(value: Any, offset: Long = 0) = Some(value.asInstanceOf[Long] + offset)
  def getSeqFilter(filter: Filter): Seq[SequenceRowFilter] = // scalastyle:off
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
      case Or(f1, f2) => normalizeFilterSet(getSeqFilter(f1) ++ getSeqFilter(f2))
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
    val segmentCounts = borders.toIterator.scanLeft[Int](minusInfCount)((count, border) => {
      if (border.start) {
        // entering new interval -> increment the count of overlaps
        count + 1
      } else {
        // leaving interval
        count - 1
      }
    })
    val borderLabels = Vector(None) ++ borders.map(b => Some(b.value)) ++ Vector(None)
    val segmentLabels =
      // zip with itself to form pairs (first, second), (second, third), (third, fourth), ...
      borderLabels
        .zip(borderLabels.drop(1))
        .map { case (low, high) => SequenceRowFilter(low, high) }
    segmentLabels
      .zip(segmentCounts.toSeq)
      // filter out empty segments
      .filter {
        case (filter, _) => filter.exclusiveEnd.forall(end => filter.inclusiveStart.forall(_ < end))
      }
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
      .collect { case (filter, count) if count == 0 => filter }

  def parseJsonValue(v: Json, columnType: String): Option[Any] =
    if (v.isNull) {
      Some(null)
    } else {
      columnType match {
        case "STRING" => v.asString
        case "DOUBLE" => v.asNumber.map(_.toDouble)
        case "LONG" => v.asNumber.flatMap(_.toLong)
        case a => throw new CdfSparkException(s"Unknown column type $a")
      }
    }

  def sequenceTypeToSparkType(columnType: String): DataType =
    columnType match {
      case "STRING" => DataTypes.StringType
      case "DOUBLE" => DataTypes.DoubleType
      case "LONG" => DataTypes.LongType
      case a => throw new CdfSparkException(s"Unknown column type $a")
    }
}

final case class ProjectedSequenceRow(rowNumber: Long, values: Array[Any])
final case class SequenceRowFilter(
    inclusiveStart: Option[Long] = None,
    exclusiveEnd: Option[Long] = None)
final case class SequenceRowDeleteSchema(rowNumber: Long)
