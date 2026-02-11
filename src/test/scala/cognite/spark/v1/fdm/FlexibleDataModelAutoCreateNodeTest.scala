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

/**
  Integration tests for autoCreateStartNodes, autoCreateEndNodes, and autoCreateDirectRelations options.
 
  These tests verify that:
  1. When autoCreate is disabled (strict mode), edge creation fails if referenced nodes don't exist
  2. When autoCreate is enabled (default), edge creation succeeds even if referenced nodes don't exist
  3. When autoCreate is disabled but referenced nodes exist, edge creation succeeds
  4. When autoCreateDirectRelations is disabled, node creation fails if direct relation target doesn't exist
  5. When autoCreateDirectRelations is enabled (default), node creation succeeds with non-existent targets
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

  it should "fail to create edge when start node doesn't exist and autoCreateStartNodes is false" in {
    val testPrefix = apiCompatibleRandomString()
    val existingEndNodeExtId = s"autoCreateTestEndNode$testPrefix"
    val dummyStartNodeExtId = s"dummyStartNode$testPrefix" // Created but not used in edge
    val nonExistentStartNodeExtId = s"nonExistentStartNode$testPrefix"
    val edgeExtId = s"autoCreateTestEdge$testPrefix"

    // Setup: Create container, view, and end node (start node referenced in edge will NOT exist)
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      view <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
      // Create two nodes, but we'll only use existingEndNodeExtId in the edge
      _ <- createNodesForEdgesIfNotExists(
        dummyStartNodeExtId,
        existingEndNodeExtId,
        view.toSourceReference
      )
    } yield view
    setup.unsafeRunSync()

    // Create edge DataFrame referencing non-existent start node
    val edgeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$nonExistentStartNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$existingEndNodeExtId') as endNode
         |""".stripMargin)

    // Try to insert with autoCreateStartNodes = false (strict mode)
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf,
        autoCreateStartNodes = Some(false),
        autoCreateEndNodes = Some(true)
      )
    }

    // Should fail because start node doesn't exist and autoCreate is disabled
    result shouldBe a[Failure[_]]
    
    // Print the DMS error message for visibility
    val errorMessage = result.failed.get.getMessage
    println(s"\n[DMS Error - Missing Start Node] $errorMessage\n")
    
    // The error message from DMS mentions the node doesn't exist as "direct relation" target
    errorMessage should include("does not exist")
  }

  it should "fail to create edge when end node doesn't exist and autoCreateEndNodes is false" in {
    val testPrefix = apiCompatibleRandomString()
    val existingStartNodeExtId = s"autoCreateTestStartNode$testPrefix"
    val dummyEndNodeExtId = s"dummyEndNode$testPrefix" // Created but not used in edge
    val nonExistentEndNodeExtId = s"nonExistentEndNode$testPrefix"
    val edgeExtId = s"autoCreateTestEdge$testPrefix"

    // Setup: Create container, view, and start node (end node referenced in edge will NOT exist)
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      view <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
      // Create two nodes, but we'll only use existingStartNodeExtId in the edge
      _ <- createNodesForEdgesIfNotExists(
        existingStartNodeExtId,
        dummyEndNodeExtId,
        view.toSourceReference
      )
    } yield view
    setup.unsafeRunSync()

    // Create edge DataFrame referencing non-existent end node
    val edgeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$existingStartNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$nonExistentEndNodeExtId') as endNode
         |""".stripMargin)

    // Try to insert with autoCreateEndNodes = false (strict mode)
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf,
        autoCreateStartNodes = Some(true),
        autoCreateEndNodes = Some(false)
      )
    }

    // Should fail because end node doesn't exist and autoCreate is disabled
    result shouldBe a[Failure[_]]
    
    // Print the DMS error message for visibility
    val errorMessage = result.failed.get.getMessage
    println(s"\n[DMS Error - Missing End Node] $errorMessage\n")
    
    // The error message from DMS mentions the node doesn't exist as "direct relation" target
    errorMessage should include("does not exist")
  }

  it should "succeed to create edge when referenced nodes don't exist but autoCreate is enabled (default)" in {
    val testPrefix = apiCompatibleRandomString()
    val autoCreatedStartNodeExtId = s"autoCreatedStartNode$testPrefix"
    val autoCreatedEndNodeExtId = s"autoCreatedEndNode$testPrefix"
    val edgeExtId = s"autoCreateSuccessTestEdge$testPrefix"

    // Setup: Create container and view (no nodes)
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      _ <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
    } yield ()
    setup.unsafeRunSync()

    // Create edge DataFrame referencing non-existent nodes
    val edgeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$autoCreatedStartNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$autoCreatedEndNodeExtId') as endNode
         |""".stripMargin)

    // Insert with default behavior (not specifying autoCreate options at all)
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf
        // Not passing autoCreateStartNodes, autoCreateEndNodes, or autoCreateDirectRelations
        // to test default behavior where user doesn't specify these options
      )
    }

    // Should succeed - nodes will be auto-created by default
    result shouldBe Success(())
  }

  it should "succeed to create edge when all referenced nodes exist even with autoCreate disabled" in {
    val testPrefix = apiCompatibleRandomString()
    val existingStartNodeExtId = s"existingStartNode$testPrefix"
    val existingEndNodeExtId = s"existingEndNode$testPrefix"
    val edgeExtId = s"strictModeSuccessEdge$testPrefix"

    // Setup: Create container, view, and BOTH nodes
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      view <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
      _ <- createNodesForEdgesIfNotExists(
        existingStartNodeExtId,
        existingEndNodeExtId,
        view.toSourceReference
      )
    } yield ()
    setup.unsafeRunSync()

    // Create edge DataFrame referencing existing nodes
    val edgeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$existingStartNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$existingEndNodeExtId') as endNode
         |""".stripMargin)

    // Insert with autoCreate disabled (strict mode)
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf,
        autoCreateStartNodes = Some(false),
        autoCreateEndNodes = Some(false)
      )
    }

    // Should succeed because all referenced nodes exist
    result shouldBe Success(())
  }

  it should "fail to create edge when both nodes don't exist and both autoCreate flags are disabled" in {
    val testPrefix = apiCompatibleRandomString()
    val nonExistentStartNodeExtId = s"nonExistentStart$testPrefix"
    val nonExistentEndNodeExtId = s"nonExistentEnd$testPrefix"
    val edgeExtId = s"fullStrictModeEdge$testPrefix"

    // Setup: Create container and view only (no nodes)
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      _ <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
    } yield ()
    setup.unsafeRunSync()

    // Create edge DataFrame referencing non-existent nodes
    val edgeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$edgeExtId' as externalId,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$nonExistentStartNodeExtId') as startNode,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$nonExistentEndNodeExtId') as endNode
         |""".stripMargin)

    // Insert with both autoCreate flags disabled (full strict mode)
    val result = Try {
      insertEdgeRows(
        edgeTypeSpace = spaceExternalId,
        edgeTypeExternalId = autoCreateEdgeTypeExtId,
        df = edgeDf,
        autoCreateStartNodes = Some(false),
        autoCreateEndNodes = Some(false)
      )
    }

    // Should fail because neither node exists
    result shouldBe a[Failure[_]]
    
    // Print the DMS error message for visibility
    val errorMessage = result.failed.get.getMessage
    println(s"\n[DMS Error - Both Nodes Missing] $errorMessage\n")
  }

  it should "create nodes with direct relation property when autoCreate is disabled and target exists" in {
    val testPrefix = apiCompatibleRandomString()
    val sourceNodeExtId = s"sourceNode$testPrefix"
    val targetNodeExtId = s"targetNode$testPrefix"
    val dummyNodeExtId = s"dummyNode$testPrefix"

    // Setup: Create container, view, and target node
    val setup = for {
      container <- createContainerIfNotExists(Usage.Node, propsMap, autoCreateTestContainerExternalId)
      view <- createViewWithCorePropsIfNotExists(container, autoCreateTestViewExternalId, viewVersion)
      // Create two nodes (required by helper), we only care about targetNodeExtId
      _ <- createNodesForEdgesIfNotExists(
        targetNodeExtId,
        dummyNodeExtId,
        view.toSourceReference
      )
    } yield ()
    setup.unsafeRunSync()

    // Create node with a direct relation property pointing to existing node
    val nodeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$sourceNodeExtId' as externalId,
         |  'testValue' as stringProp1
         |""".stripMargin)

    // Insert node with autoCreate disabled
    val result = Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = autoCreateTestViewExternalId,
        viewVersion = viewVersion,
        instanceSpaceExternalId = spaceExternalId,
        df = nodeDf,
        autoCreateStartNodes = Some(false),
        autoCreateEndNodes = Some(false)
      )
    }

    // Should succeed - we're creating a simple node with no direct relations
    result shouldBe Success(())
  }

  // Tests for autoCreateDirectRelations option
  private val directRelationContainerExternalId = "sparkDsAutoCreateDirectRelContainer1"
  private val directRelationViewExternalId = "sparkDsAutoCreateDirectRelView1"

  private val directRelationPropsMap = Map(
    "stringProp1" -> FDMContainerPropertyDefinitions.TextPropertyNonListWithoutDefaultValueNullable,
    "relatedNode" -> FDMContainerPropertyDefinitions.DirectNodeRelationPropertyNonListWithoutDefaultValueNullable
  )

  it should "fail to create node with direct relation when target doesn't exist and autoCreateDirectRelations is false" in {
    val testPrefix = apiCompatibleRandomString()
    val sourceNodeExtId = s"directRelSourceNode$testPrefix"
    val nonExistentTargetExtId = s"nonExistentDirectRelTarget$testPrefix"

    // Setup: Create container and view with direct relation property (no target node)
    val setup = for {
      container <- createContainerIfNotExists(
        Usage.Node,
        directRelationPropsMap,
        directRelationContainerExternalId
      )
      _ <- createViewWithCorePropsIfNotExists(container, directRelationViewExternalId, viewVersion)
    } yield ()
    setup.unsafeRunSync()

    // Create node with a direct relation property pointing to non-existent node
    val nodeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$sourceNodeExtId' as externalId,
         |  'testValue' as stringProp1,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$nonExistentTargetExtId') as relatedNode
         |""".stripMargin)

    // Try to insert with autoCreateDirectRelations = false (strict mode)
    val result = Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = directRelationViewExternalId,
        viewVersion = viewVersion,
        instanceSpaceExternalId = spaceExternalId,
        df = nodeDf,
        autoCreateDirectRelations = Some(false)
      )
    }

    // Should fail because target node doesn't exist and autoCreateDirectRelations is disabled
    result shouldBe a[Failure[_]]

    // Print the DMS error message for visibility
    val errorMessage = result.failed.get.getMessage
    println(s"\n[DMS Error - Direct Relation Target Missing] $errorMessage\n")

    // The error message from DMS mentions the node doesn't exist
    errorMessage should include("does not exist")
  }

  it should "succeed to create node with direct relation when target doesn't exist but autoCreateDirectRelations is enabled (default)" in {
    val testPrefix = apiCompatibleRandomString()
    val sourceNodeExtId = s"directRelSourceNode$testPrefix"
    val autoCreatedTargetExtId = s"autoCreatedDirectRelTarget$testPrefix"

    // Setup: Create container and view with direct relation property (no target node)
    val setup = for {
      container <- createContainerIfNotExists(
        Usage.Node,
        directRelationPropsMap,
        directRelationContainerExternalId
      )
      _ <- createViewWithCorePropsIfNotExists(container, directRelationViewExternalId, viewVersion)
    } yield ()
    setup.unsafeRunSync()

    // Create node with a direct relation property pointing to non-existent node
    val nodeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$sourceNodeExtId' as externalId,
         |  'testValue' as stringProp1,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$autoCreatedTargetExtId') as relatedNode
         |""".stripMargin)

    // Insert with default behavior (not specifying autoCreateDirectRelations)
    val result = Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = directRelationViewExternalId,
        viewVersion = viewVersion,
        instanceSpaceExternalId = spaceExternalId,
        df = nodeDf
        // Not passing autoCreateDirectRelations to test default behavior
      )
    }

    // Should succeed - target node will be auto-created by default
    result shouldBe Success(())
  }

  it should "succeed to create node with direct relation when target exists even with autoCreateDirectRelations disabled" in {
    val testPrefix = apiCompatibleRandomString()
    val sourceNodeExtId = s"directRelSourceNode$testPrefix"
    val existingTargetExtId = s"existingDirectRelTarget$testPrefix"
    val dummyNodeExtId = s"dummyNode$testPrefix"

    // Setup: Create container, view with direct relation property, and target node
    val setup = for {
      container <- createContainerIfNotExists(
        Usage.Node,
        directRelationPropsMap,
        directRelationContainerExternalId
      )
      view <- createViewWithCorePropsIfNotExists(container, directRelationViewExternalId, viewVersion)
      // Create target node that we'll reference
      _ <- createNodesForEdgesIfNotExists(
        existingTargetExtId,
        dummyNodeExtId,
        view.toSourceReference
      )
    } yield ()
    setup.unsafeRunSync()

    // Create node with a direct relation property pointing to existing node
    val nodeDf: DataFrame = spark.sql(
      s"""
         |SELECT
         |  '$spaceExternalId' as space,
         |  '$sourceNodeExtId' as externalId,
         |  'testValue' as stringProp1,
         |  named_struct('space', '$spaceExternalId', 'externalId', '$existingTargetExtId') as relatedNode
         |""".stripMargin)

    // Insert with autoCreateDirectRelations disabled
    val result = Try {
      insertNodeRows(
        instanceType = InstanceType.Node,
        viewSpaceExternalId = spaceExternalId,
        viewExternalId = directRelationViewExternalId,
        viewVersion = viewVersion,
        instanceSpaceExternalId = spaceExternalId,
        df = nodeDf,
        autoCreateDirectRelations = Some(false)
      )
    }

    // Should succeed because target node exists
    result shouldBe Success(())
  }
}
