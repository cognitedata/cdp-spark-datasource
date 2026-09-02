package cognite.spark.v1.fdm

import cats.effect.unsafe.implicits.global
import cognite.spark.v1.SparkTest
import cognite.spark.v1.fdm.utils.FDMSparkDataframeTestOperations._
import cognite.spark.v1.fdm.utils.FDMTestConstants._
import cognite.spark.v1.fdm.utils.{FDMContainerPropertyDefinitions, FDMTestInitializer}
import com.cognite.sdk.scala.v1.SpaceCreateDefinition
import com.cognite.sdk.scala.v1.fdm.common.Usage
import com.cognite.sdk.scala.v1.fdm.instances.InstanceType
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success, Try}

/*
  Integration tests for autoCreateStartNodes, autoCreateEndNodes, and autoCreateDirectRelations options.

  Uses table-driven tests to cover all combinations of:
  - Whether referenced nodes exist
  - Auto-create option settings (true, false, or not specified)
  - Expected outcome (success or failure)
 */
class FlexibleDataModelAutoCreateNodeTest
    extends FlatSpec
    with Matchers
    with SparkTest
    with FDMTestInitializer {

  private val autoCreateTestContainerExternalId = "sparkDsAutoCreateTestContainer1"
  private val autoCreateTestViewExternalId = "sparkDsAutoCreateTestView1"
  private val autoCreateEdgeTypeExtId = "sparkDsAutoCreateTestEdgeType1"

  private val propsMap = Map(
    "stringProp1" -> FDMContainerPropertyDefinitions.TextPropertyNonListWithoutDefaultValueNullable
  )

  // Create space if not exists
  client.spacesv3.createItems(Seq(SpaceCreateDefinition(spaceExternalId))).unsafeRunSync()

  // Edge Auto-Create Tests

  case class EdgeTestCase(
      description: String,
      startNodeExists: Boolean,
      endNodeExists: Boolean,
      autoCreateStartNodes: Option[Boolean],
      autoCreateEndNodes: Option[Boolean],
      shouldSucceed: Boolean
  )

  private val edgeTestCases = Seq(
    // When autoCreate is disabled (false) and node doesn't exist -> should fail
    EdgeTestCase(
      "fail when start node missing and autoCreateStartNodes=false",
      startNodeExists = false,
      endNodeExists = true,
      autoCreateStartNodes = Some(false),
      autoCreateEndNodes = Some(true),
      shouldSucceed = false
    ),
    EdgeTestCase(
      "fail when end node missing and autoCreateEndNodes=false",
      startNodeExists = true,
      endNodeExists = false,
      autoCreateStartNodes = Some(true),
      autoCreateEndNodes = Some(false),
      shouldSucceed = false
    ),
    EdgeTestCase(
      "fail when both nodes missing and both autoCreate=false",
      startNodeExists = false,
      endNodeExists = false,
      autoCreateStartNodes = Some(false),
      autoCreateEndNodes = Some(false),
      shouldSucceed = false
    ),
    // When autoCreate is enabled (true or not specified) and node doesn't exist -> should succeed
    EdgeTestCase(
      "succeed when nodes missing but autoCreate not specified (default true)",
      startNodeExists = false,
      endNodeExists = false,
      autoCreateStartNodes = None,
      autoCreateEndNodes = None,
      shouldSucceed = true
    ),
    EdgeTestCase(
      "succeed when nodes missing but autoCreate=true",
      startNodeExists = false,
      endNodeExists = false,
      autoCreateStartNodes = Some(true),
      autoCreateEndNodes = Some(true),
      shouldSucceed = true
    ),
    // When autoCreate is disabled but nodes exist -> should succeed
    EdgeTestCase(
      "succeed when all nodes exist even with autoCreate=false",
      startNodeExists = true,
      endNodeExists = true,
      autoCreateStartNodes = Some(false),
      autoCreateEndNodes = Some(false),
      shouldSucceed = true
    ),
    // Mixed: one node exists, autoCreate disabled for the existing one
    EdgeTestCase(
      "succeed when start exists, end missing, autoCreateEndNodes=true",
      startNodeExists = true,
      endNodeExists = false,
      autoCreateStartNodes = Some(false),
      autoCreateEndNodes = Some(true),
      shouldSucceed = true
    ),
    EdgeTestCase(
      "succeed when end exists, start missing, autoCreateStartNodes=true",
      startNodeExists = false,
      endNodeExists = true,
      autoCreateStartNodes = Some(true),
      autoCreateEndNodes = Some(false),
      shouldSucceed = true
    )
  )

  edgeTestCases.foreach { tc =>
    it should tc.description in {
      runEdgeAutoCreateTest(tc)
    }
  }

  private def runEdgeAutoCreateTest(tc: EdgeTestCase): Unit = {
    val testSuffix = apiCompatibleRandomString()
    val startNodeExtId = s"startNode$testSuffix"
    val endNodeExtId = s"endNode$testSuffix"
    val edgeExtId = s"edge$testSuffix"

    // Setup: Create container and view
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      view <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
      // Create nodes based on test case requirements
      _ <- if (tc.startNodeExists || tc.endNodeExists) {
        // Create nodes that should exist
        val existingStart = if (tc.startNodeExists) startNodeExtId else s"dummy1$testSuffix"
        val existingEnd = if (tc.endNodeExists) endNodeExtId else s"dummy2$testSuffix"
        createNodesForEdgesIfNotExists(existingStart, existingEnd, view.toSourceReference)
      } else {
        cats.effect.IO.pure(())
      }
    } yield ()
    setup.unsafeRunSync()

    // Create edge DataFrame
    val edgeDf: DataFrame = spark.sql(s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$startNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$endNodeExtId') as endNode
         |""".stripMargin)

    // Execute insertion
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf,
        autoCreateStartNodes = tc.autoCreateStartNodes,
        autoCreateEndNodes = tc.autoCreateEndNodes
      )
    }

    // Assert
    withClue(s"Test case: ${tc.description}") {
      if (tc.shouldSucceed) {
        result shouldBe Success(())
      } else {
        result shouldBe a[Failure[_]]
        result.failed.get.getMessage should include("does not exist")
      }
    }
  }

  // Direct Relation Auto-Create Tests

  private val directRelationContainerExternalId = "sparkDsAutoCreateDirectRelContainer1"
  private val directRelationViewExternalId = "sparkDsAutoCreateDirectRelView1"

  private val directRelationPropsMap = Map(
    "stringProp1" -> FDMContainerPropertyDefinitions.TextPropertyNonListWithoutDefaultValueNullable,
    "relatedNode" -> FDMContainerPropertyDefinitions.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable
  )

  case class DirectRelationTestCase(
      description: String,
      targetNodeExists: Boolean,
      autoCreateDirectRelations: Option[Boolean],
      shouldSucceed: Boolean
  )

  private val directRelationTestCases = Seq(
    // When autoCreate is disabled and target doesn't exist -> should fail
    DirectRelationTestCase(
      "fail when direct relation target missing and autoCreateDirectRelations=false",
      targetNodeExists = false,
      autoCreateDirectRelations = Some(false),
      shouldSucceed = false
    ),
    // When autoCreate is enabled (true or not specified) and target doesn't exist -> should succeed
    DirectRelationTestCase(
      "succeed when target missing but autoCreateDirectRelations not specified (default true)",
      targetNodeExists = false,
      autoCreateDirectRelations = None,
      shouldSucceed = true
    ),
    DirectRelationTestCase(
      "succeed when target missing but autoCreateDirectRelations=true",
      targetNodeExists = false,
      autoCreateDirectRelations = Some(true),
      shouldSucceed = true
    ),
    // When autoCreate is disabled but target exists -> should succeed
    DirectRelationTestCase(
      "succeed when target exists even with autoCreateDirectRelations=false",
      targetNodeExists = true,
      autoCreateDirectRelations = Some(false),
      shouldSucceed = true
    )
  )

  directRelationTestCases.foreach { tc =>
    it should tc.description in {
      runDirectRelationAutoCreateTest(tc)
    }
  }

  private def runDirectRelationAutoCreateTest(tc: DirectRelationTestCase): Unit = {
    val testSuffix = apiCompatibleRandomString()
    val sourceNodeExtId = s"sourceNode$testSuffix"
    val targetNodeExtId = s"targetNode$testSuffix"

    // Setup: Create container and view with direct relation property
    val setup = for {
      container <- createContainerIfNotExists(
        Usage.Node,
        directRelationPropsMap,
        directRelationContainerExternalId
      )
      view <- createViewWithCorePropsIfNotExists(container, directRelationViewExternalId, viewVersion)
      // Create target node if it should exist
      _ <- if (tc.targetNodeExists) {
        createNodesForEdgesIfNotExists(targetNodeExtId, s"dummy$testSuffix", view.toSourceReference)
      } else {
        cats.effect.IO.pure(())
      }
    } yield ()
    setup.unsafeRunSync()

    // Create node with direct relation property
    val nodeDf: DataFrame = spark.sql(s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$sourceNodeExtId' as externalId,
         |  'testValue' as stringProp1,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$targetNodeExtId') as relatedNode
         |""".stripMargin)

    // Execute insertion
    val result = Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = directRelationViewExternalId,
        viewVersion = viewVersion,
        instanceSpaceExternalId = spaceExternalId,
        df = nodeDf,
        autoCreateDirectRelations = tc.autoCreateDirectRelations
      )
    }

    // Assert
    withClue(s"Test case: ${tc.description}") {
      if (tc.shouldSucceed) {
        result shouldBe Success(())
      } else {
        result shouldBe a[Failure[_]]
        result.failed.get.getMessage should include("does not exist")
      }
    }
  }
}
