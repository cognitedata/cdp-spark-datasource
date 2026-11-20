package cognite.spark.v1

import cats.effect.IO
import cognite.spark.v1.fdm.FlexibleDataModelQueryUtils
import com.cognite.sdk.scala.v1.GenericClient
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ViewCorePropertyDefinition
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDefinition.{EdgeDefinition, NodeDefinition}
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import io.circe.Json
import io.circe.syntax._

/**
  * Serves DM instances in batch-api wire shape. The real batch API is not available in the Scala
  * SDK yet; this converts DM query results (and view definitions) into the poll/cursors/read JSON
  * that [[BatchApiRelation]] already understands, so batch and non-sync FDM relations can be
  * compared end-to-end.
  */
object BatchApiViaDm {
  type Typing = Map[String, Map[String, Map[String, TypePropertyDefinition]]]

  /** Begin-response typing for a single view (core properties only; connections are skipped). */
  def typingFromView(view: ViewDefinition): Typing = {
    val props = view.properties.collect {
      case (name, p: ViewCorePropertyDefinition) =>
        name -> TypePropertyDefinition(
          nullable = p.nullable,
          autoIncrement = p.autoIncrement,
          defaultValue = p.defaultValue,
          description = p.description,
          name = p.name,
          `type` = p.`type`
        )
    }
    Map(view.space -> Map(s"${view.externalId}/${view.version}" -> props))
  }

  /**
    * Encode an instance the way batch-api / DM read responses do. The SDK's InstanceDefinition
    * encoder omits node `type` and edge `startNode`/`endNode`, which the typed decoder expects.
    */
  def encodeInstance(instance: InstanceDefinition): Json =
    instance match {
      case n: NodeDefinition =>
        Json
          .obj(
            "instanceType" -> n.instanceType.asJson,
            "space" -> n.space.asJson,
            "externalId" -> n.externalId.asJson,
            "createdTime" -> n.createdTime.asJson,
            "lastUpdatedTime" -> n.lastUpdatedTime.asJson,
            "deletedTime" -> n.deletedTime.asJson,
            "version" -> n.version.asJson,
            "properties" -> n.properties.asJson,
            "type" -> n.`type`.asJson
          )
          .dropNullValues
      case e: EdgeDefinition =>
        Json
          .obj(
            "instanceType" -> e.instanceType.asJson,
            "type" -> e.`type`.asJson,
            "space" -> e.space.asJson,
            "externalId" -> e.externalId.asJson,
            "createdTime" -> e.createdTime.asJson,
            "lastUpdatedTime" -> e.lastUpdatedTime.asJson,
            "deletedTime" -> e.deletedTime.asJson,
            "version" -> e.version.asJson,
            "properties" -> e.properties.asJson,
            "startNode" -> e.startNode.asJson,
            "endNode" -> e.endNode.asJson
          )
          .dropNullValues
    }

  def encodeReadResponse(items: Seq[InstanceDefinition], nextReadCursor: Option[String]): Json =
    Json
      .obj(
        "items" -> items.map(encodeInstance).asJson,
        "nextReadCursor" -> nextReadCursor.asJson
      )
      .dropNullValues

  /**
    * Split instances into named read pages linked by `nextReadCursor`. Returns the single starting
    * cursor (later pages are reached via next) and a map of cursor -> ReadResponse JSON body,
    * suitable for a fake swamp `/read` endpoint.
    */
  def asSwampPages(
      items: Seq[InstanceDefinition],
      pageSize: Int
  ): (Seq[String], Map[String, String]) = {
    require(pageSize > 0, "pageSize must be positive")
    if (items.isEmpty) {
      (Seq.empty, Map.empty)
    } else {
      val pages = items.grouped(pageSize).toVector
      val cursors = pages.indices.map(i => s"dm-cursor-$i")
      val bodies = pages.zipWithIndex.map {
        case (pageItems, idx) =>
          val next = if (idx + 1 < pages.size) Some(cursors(idx + 1)) else None
          cursors(idx) -> encodeReadResponse(pageItems, next).noSpaces
      }.toMap
      // Only the first cursor is a starting cursor; the rest are follow-on pages.
      (Seq(cursors.head), bodies)
    }
  }

  /**
    * Query instances for a view the same way non-sync [[fdm.FlexibleDataModelCorePropertyRelation]]
    * does (hasData on the view, no sync cursor).
    */
  def queryInstances(
      client: GenericClient[IO],
      view: ViewDefinition,
      instanceType: InstanceType,
      limit: Option[Int] = None,
      batchSize: Option[Int] = None
  ): IO[Vector[InstanceDefinition]] = {
    val viewReference: ViewReference = view.toSourceReference
    val propertyNames = view.properties.collect {
      case (name, _: ViewCorePropertyDefinition) => name
    }.toArray
    val tableExpression = FlexibleDataModelQueryUtils.generateTableExpression(
      instanceType,
      FlexibleDataModelQueryUtils.queryFilterWithHasData(None, Some(viewReference)),
      limit
    )
    val selectExpression = SelectExpression(
      sources =
        FlexibleDataModelQueryUtils.sourceReference(instanceType, Some(viewReference), propertyNames)
    )
    client.instances
      .queryStream(
        inputTableExpression = tableExpression,
        inputSelectExpression = selectExpression,
        limit = limit,
        batchSize = batchSize
      )
      .compile
      .toVector
  }
}
