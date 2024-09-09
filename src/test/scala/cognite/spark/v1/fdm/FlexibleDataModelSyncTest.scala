package cognite.spark.v1.fdm

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cognite.spark.v1.SparkTest
import cognite.spark.v1.fdm.utils.FDMSparkDataframeTestOperations._
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import cognite.spark.v1.fdm.utils.{FDMContainerPropertyDefinitions, FDMSparkDataframeTestOperations, FDMTestInitializer}
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.common.properties.PropertyDefinition.ContainerPropertyDefinition
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances._
import com.cognite.sdk.scala.v1.fdm.views.{ViewDefinition, ViewReference}
import org.scalatest.{FlatSpec, Matchers}

class FlexibleDataModelSyncTest extends FlatSpec
  with Matchers
  with SparkTest
  with FDMTestInitializer {

  private val containerSyncTest = "sparkDsTestContainerForSync"
  private val viewSyncTest = "sparkDsTestViewForSync"

  private def setupInstancesForSync(viewRef: ViewReference, instances: Int): IO[Seq[String]] = {
    val items =
      for (i <- 1 to instances)
        yield
          NodeWrite(
            spaceExternalId,
            "external" + i,
            sources = Some(
              Seq(EdgeOrNodeData(
                viewRef,
                Some(Map(
                  "stringProp" -> Some(InstancePropertyValue.String(System.nanoTime().toString)),
                  "longProp" -> Some(InstancePropertyValue.Int64(i.toLong))
                ))
              ))),
            `type` = None
          )
    client.instances
      .createItems(InstanceCreate(items = items))
      .map(_.collect { case n: SlimNodeOrEdge.SlimNodeDefinition => n.externalId })
      .map(_.distinct)
  }

  private def setupSyncTest: IO[(ViewDefinition, Seq[String])] = {
    val containerProps: Map[String, ContainerPropertyDefinition] = Map(
      "stringProp" -> FDMContainerPropertyDefinitions.TextPropertyNonListWithoutDefaultValueNullable,
      "longProp" -> FDMContainerPropertyDefinitions.Float64NonListWithoutDefaultValueNullable,
    )

    for {
      containerDefinition <- createContainerIfNotExists(Usage.All, containerProps, containerSyncTest)
      viewDefinition <- createViewWithCorePropsIfNotExists(
        containerDefinition,
        viewSyncTest,
        viewVersion)
      extIds <- setupInstancesForSync(viewDefinition.toSourceReference, 50)
    } yield (viewDefinition, extIds)
  }


  it should "be able to fetch more data with cursor when syncing" in {
    val (viewDefinition, modifiedExternalIds) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = ""
    )

    syncDf.createTempView(s"sync_empty_cursor")
    val syncedNodes = spark.sql("select * from sync_empty_cursor").collect()
    val syncedExternalIds = toExternalIds(syncedNodes)
    val lastRow = syncedNodes.last
    val cursor = lastRow.getString(lastRow.schema.fieldIndex("metadata.cursor"))

    val createdTime = lastRow.getLong(lastRow.schema.fieldIndex("node.createdTime"))
    createdTime should be > 1L
    val lastUpdated = lastRow.getLong(lastRow.schema.fieldIndex("node.lastUpdatedTime"))
    lastUpdated should be >= createdTime
    val deletedTime = lastRow.getLong(lastRow.schema.fieldIndex("node.deletedTime"))
    deletedTime shouldBe 0L
    val version = lastRow.getLong(lastRow.schema.fieldIndex("node.version"))
    version should be > 0L

    (syncedExternalIds should contain).allElementsOf(modifiedExternalIds)

    val syncNext = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = cursor)

    syncNext.createTempView(s"sync_next_cursor")
    var syncedNextNodes = spark.sql("select * from sync_next_cursor").collect()
    syncedNextNodes.length shouldBe 0

    // Add 10 nodes and verify we can sync them out with same cursor
    setupInstancesForSync(viewDefinition.toSourceReference, 10).unsafeRunSync()
    syncedNextNodes = spark.sql("select * from sync_next_cursor").collect()
    syncedNextNodes.length shouldBe 10
  }

  it should "respect filters in sync queries" in {
    val (viewDefinition, _) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = ""
    )

    syncDf.createTempView(s"sync_empty_cursor_with_filter")
    val syncedNodes = spark
      .sql("select * from sync_empty_cursor_with_filter where longProp > 10L and longProp <= 50")
      .collect()
    syncedNodes.length shouldBe 40

    val syncedNodes2 = spark
      .sql(
        "select * from sync_empty_cursor_with_filter where " +
          "`node.lastUpdatedTime` > 10L and " +
          "longProp > 0 and " +
          "space = '" + spaceExternalId + "' and " +
          "`node.deletedTime` = 0")
      .collect()
    syncedNodes2.length shouldBe 50

    val syncedNodes3 = spark
      .sql(
        "select * from sync_empty_cursor_with_filter where " +
          "`node.lastUpdatedTime` > 10L and " +
          "longProp > 0 and " +
          "space = 'nonexistingspace'")
      .collect()
    syncedNodes3.length shouldBe 0
  }

  it should "sync with old cursor " in {
    // Let us see what happens when this cursor gets more than 3 days..
    val oldCursorValue = "\"Z0FBQUFBQmw0RjJXenpCbHl3Ny02MzRRN21lTEVkdm5hU3hpQTM4d05ERkwxV2xNSG5" +
      "wa2ltOHhXMXloWC05akN6TDEzWGExMkkwZlpocGVhdVZXSDBGMVpEdmIwSTlzVDhQQUVhOHBNV2pCNUFNMFBsREg" +
      "4WjlEU3RacFFMbS1MM3hZaGVkV3ZFcVhoUy13R0NlODEzaEhlZUhUbVoxeTZ3bWVCX0tnSWdXRkphcjJIS2hfemh" +
      "pM216WktROG9RdjdqVmRWeFRMdDZ5TVdVNG1iNGo2X3J1VVVVRy10ZnowYUozSnV0R2ozZjd6YU1mMjBNVnc1eWN" +
      "tX1hmbF80RmY0RkdVRk5DVTI0UE9scW5rbkF1MEZIUHBKUklERDVnRDJ5ZWhXVGVwNE4wcVBpeVV1amdGSDZKeTd" +
      "nanhkRTNiZ2QtUTdWdGdhbUpHakdEWXpQRmNueEFGdGd3UTBHdHZpY3hZdnpHdk16QVBlNE8wSlZzUGk0d2hOV01" +
      "jRUFQdjNPNE9mQ0g0MG11S1NYMzJyWnczRkhBWG00WWJCRU1jUT09\""

    val (viewDefinition, modifiedExternalIds) = setupSyncTest.unsafeRunSync()
    val view = viewDefinition.toSourceReference
    val syncDf = syncRows(
      instanceType = InstanceType.Node,
      viewSpaceExternalId = spaceExternalId,
      viewExternalId = view.externalId,
      viewVersion = view.version,
      cursor = oldCursorValue
    )

    syncDf.createTempView(s"sync_old_cursor")
    val syncedNodes = spark.sql("select * from sync_old_cursor").collect()
    val syncedExternalIds = toExternalIds(syncedNodes)

    (syncedExternalIds should contain).allElementsOf(modifiedExternalIds)
  }
}
