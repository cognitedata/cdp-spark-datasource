package cognite.spark.v1.fdm

import cats.effect.IO
import cats.implicits.toTraverseOps
import cognite.spark.v1.fdm.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory.ConnectionConfig
import cognite.spark.v1.fdm.RelationUtils.FlexibleDataModelRelationUtils.{
  createConnectionInstances,
  createEdgeDeleteData
}
import cognite.spark.v1.{CdfSparkException, RelationConfig}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceCreate, InstanceFilterRequest, InstanceType}
import fs2.Stream
import io.circe.Json
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Flexible Data Model Relation for Connection instances (i.e edges without properties)
  *
  * @param config common relation configs
  * @param connectionConfig connection definition info
  * @param sqlContext sql context
  */
private[spark] class FlexibleDataModelConnectionRelation(
    config: RelationConfig,
    connectionConfig: ConnectionConfig)(val sqlContext: SQLContext)
    extends FlexibleDataModelBaseRelation(config, sqlContext) {

  private val instanceSpace = connectionConfig.instanceSpace

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
              DirectRelationReference(
                space = connectionConfig.edgeTypeSpace,
                externalId = connectionConfig.edgeTypeExternalId
              ),
              firstRow.schema,
              rows,
              instanceSpace
            )
          )
          .flatMap { instances =>
            val instanceCreate = InstanceCreate(
              items = instances,
              replace = Some(false),
              autoCreateStartNodes = Some(true),
              autoCreateEndNodes = Some(true)
            )
            client.instances.createItems(instanceCreate)
          }
          .flatMap(results =>
            for {
              _ <- incMetrics(itemsUpserted, results.length)
              _ <- incMetrics(itemsUpsertedNoop, results.count(!_.wasModified))
            } yield ())
      case None => incMetrics(itemsUpserted, 0)
    }

  override def delete(rows: Seq[Row]): IO[Unit] =
    rows.headOption match {
      case Some(firstRow) =>
        IO.fromEither(createEdgeDeleteData(firstRow.schema, rows, instanceSpace))
          .flatMap(client.instances.delete)
          .flatMap(results => incMetrics(itemsDeleted, results.length))
      case None => incMetrics(itemsDeleted, 0)
    }

  override def getStreams(filters: Array[Filter], selectedColumns: Array[String])(
      client: GenericClient[IO]): Seq[Stream[IO, ProjectedFlexibleDataModelInstance]] = {
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
      limit = config.limitPerPartition,
      cursor = None,
      sources = None,
      includeTyping = Some(true),
      debug = optionalDebug(config.sendDebugFlag)
    )

    Vector(
      client.instances
        .filterStream(filterReq, config.limitPerPartition)
        .map(toProjectedInstance(_, None, selectedFields)))
  }

  private def extractFilters(filters: Array[Filter]): Either[CdfSparkException, FilterDefinition] = {
    val edgeTypeFilter = FilterDefinition.Equals(
      property = Vector("edge", "type"),
      value = FilterValueDefinition.Object(
        Json.obj(
          "space" -> Json.fromString(connectionConfig.edgeTypeSpace),
          "externalId" -> Json.fromString(connectionConfig.edgeTypeExternalId)))
    )

    if (filters.isEmpty) {
      Right(edgeTypeFilter)
    } else {
      filters.toVector.traverse(toFilter(InstanceType.Edge, _, None)).map { f =>
        FilterDefinition.And(edgeTypeFilter +: f)
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
}
