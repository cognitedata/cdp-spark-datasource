package cognite.spark.v1

import cats.effect.IO
import cats.implicits.{toBifunctorOps, toTraverseOps}
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelation.ConnectionConfig
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createConnectionInstances, createEdgeDeleteData}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceCreate, InstanceFilterRequest, InstanceType}
import fs2.Stream
import io.circe.{Json, JsonObject}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.annotation.nowarn
import scala.util.Try

/**
  * FlexibleDataModels Relation for Connection definitions
  *
  * @param config common relation configs
  * @param connectionConfig connection config
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelConnectionRelation(
    config: RelationConfig,
    connectionConfig: ConnectionConfig)(val sqlContext: SQLContext)
    extends FlexibleDataModelBaseRelation(config, sqlContext) {

  private val connectionInstanceSchema = DataTypes.createStructType(
    Array(
      DataTypes.createStructField("space", DataTypes.StringType, false),
      DataTypes.createStructField("externalId", DataTypes.StringType, false),
      relationReferenceSchema("startNode", nullable = false),
      relationReferenceSchema("endNode", nullable = false)
    )
  )

  override def schema: StructType = connectionInstanceSchema

  override def upsert(rows: Seq[Row]): IO[Unit] =
    rows.headOption match {
      case Some(firstRow) =>
        IO.fromEither(
            createConnectionInstances(
              edgeType = DirectRelationReference(
                space = connectionConfig.edgeTypeSpace,
                externalId = connectionConfig.edgeTypeExternalId
              ),
              rows,
              dataRowSchema = firstRow.schema,
              connectionInstanceSchema
            )
          )
          .flatMap { instances =>
            val instanceCreate = InstanceCreate(
              items = instances,
              replace = Some(true)
            )
            client.instances.createItems(instanceCreate)
          }
          .flatMap(results => incMetrics(itemsUpserted, results.length))
      case None => incMetrics(itemsUpserted, 0)
    }

  override def delete(rows: Seq[Row]): IO[Unit] =
    rows.headOption match {
      case Some(firstRow) =>
        IO.fromEither(createEdgeDeleteData(firstRow.schema, rows))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case None => incMetrics(itemsDeleted, 0)
    }

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO],
      limit: Option[Int],
      numPartitions: Int): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
    val selectedFields = if (selectedColumns.isEmpty) {
      schema.fieldNames
    } else {
      selectedColumns
    }

    val instanceFilters = extractFilters(filters) match {
      case Right(v) => v
      case Left(err) => throw err
    }

    val filterReq = InstanceFilterRequest(
      instanceType = Some(InstanceType.Edge),
      filter = Some(instanceFilters),
      sort = None,
      limit = limit,
      cursor = None,
      sources = None,
      includeTyping = Some(true)
    )

    Vector(
      client.instances
        .filterStream(filterReq, limit)
        .map(toProjectedInstance(_, selectedFields)))
  }

  private def extractFilters(filters: Array[Filter]): Either[CdfSparkException, FilterDefinition] = {
    val edgeTypeFilter = FilterDefinition.And(
      Vector(
        FilterDefinition.Equals(
          property = Vector("edge", "space"),
          value = FilterValueDefinition.String(connectionConfig.edgeTypeSpace)
        ),
        FilterDefinition.Nested(
          scope = Vector("edge", "type"),
          filter = FilterDefinition.Equals(
            property = Vector("node", "externalId"),
            value = FilterValueDefinition.String(connectionConfig.edgeTypeExternalId)
          )
        )
      )
    )

    if (filters.isEmpty) {
      Right(edgeTypeFilter)
    } else {
      filters.toVector.traverse(toInstanceFilter).map { f =>
        edgeTypeFilter.copy(filters = edgeTypeFilter.filters ++ f)
      }
    }
  }

  override def update(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Update is not supported for flexible data model connection instances. Use upsert instead."))

  override def insert(rows: Seq[Row]): IO[Unit] =
    IO.raiseError[Unit](
      new CdfSparkException(
        "Create is not supported for flexible data model connection instances. Use upsert instead."))

  private def toInstanceFilter(sparkFilter: Filter): Either[CdfSparkException, FilterDefinition] =
    sparkFilter match {
      case EqualTo(attribute, value: String) if attribute.equalsIgnoreCase("space") =>
        Right(
          FilterDefinition.Equals(
            property = Vector("edge", "space"),
            value = FilterValueDefinition.String(value)
          )
        )
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("startNode") =>
        toDirectRelationReferenceFilter("startNode", value)
      case EqualTo(attribute, value: GenericRowWithSchema) if attribute.equalsIgnoreCase("endNode") =>
        toDirectRelationReferenceFilter("endNode", value)
      case f =>
        Left(
          new CdfSparkIllegalArgumentException(
            s"Unsupported filter '${f.getClass.getSimpleName}': ${String.valueOf(f)}"))
    }
  // scalastyle:on cyclomatic.complexity

  private def toDirectRelationReferenceFilter(
      @nowarn attribute: String,
      struct: GenericRowWithSchema): Either[CdfSparkException, FilterDefinition] =
    Try {
      val space = struct.getString(struct.fieldIndex("space"))
      val externalId = struct.getString(struct.fieldIndex("externalId"))
      FilterDefinition.Equals(
        property = Vector("edge", "startNode"),
        value = FilterValueDefinition.Object(
          Json.fromJsonObject(
            JsonObject(
              ("space", Json.fromString(space)),
              ("externalId", Json.fromString(externalId))
            )
          )
        )
      )
    }.toEither.leftMap { _ =>
      new CdfSparkIllegalArgumentException(
        s"""Expecting a struct with 'space' & 'externalId' attributes, but found: ${struct.json}
           |""".stripMargin
      )
    }
}
