package cognite.spark.v1

import cats.effect.IO
import cats.implicits.toTraverseOps
import cognite.spark.v1.FlexibleDataModelBaseRelation.ProjectedFlexibleDataModelInstance
import cognite.spark.v1.FlexibleDataModelRelationFactory.ConnectionConfig
import cognite.spark.v1.FlexibleDataModelRelationUtils.{createConnectionInstances, createEdgeDeleteData}
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.DirectRelationReference
import com.cognite.sdk.scala.v1.fdm.common.filters.{FilterDefinition, FilterValueDefinition}
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceCreate, InstanceFilterRequest, InstanceType}
import fs2.Stream
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
        IO.fromEither(createEdgeDeleteData(None, firstRow.schema, rows))
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
    val edgeTypeFilter = FilterDefinition.Equals(
      property = Vector("edge", "type"),
      value = FilterValueDefinition.StringList(
        Vector(connectionConfig.edgeTypeSpace, connectionConfig.edgeTypeExternalId))
    )

    if (filters.isEmpty) {
      Right(edgeTypeFilter)
    } else {
      filters.toVector.traverse(toNodeOrEdgeAttributeFilter(InstanceType.Edge, _)).map { f =>
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
  // scalastyle:on cyclomatic.complexity
}
