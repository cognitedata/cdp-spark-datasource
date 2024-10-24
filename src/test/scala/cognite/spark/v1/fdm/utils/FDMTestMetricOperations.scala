package cognite.spark.v1.fdm.utils

import cognite.spark.v1.SparkTest
import cognite.spark.v1.fdm.FlexibleDataModelRelationFactory
import com.cognite.sdk.scala.v1.fdm.views.ViewDefinition

object FDMTestMetricOperations extends SparkTest {
  def getUpsertedMetricsCount(edgeTypeSpace: String, edgeTypeExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$edgeTypeSpace-$edgeTypeExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

  def getUpsertedMetricsCountForModel(modelSpace: String, modelExternalId: String): Long =
    getNumberOfRowsUpserted(
      s"$modelSpace-$modelExternalId",
      FlexibleDataModelRelationFactory.ResourceType)

  def getUpsertedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsUpserted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  def getReadMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsRead(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)

  def getDeletedMetricsCount(viewDef: ViewDefinition): Long =
    getNumberOfRowsDeleted(
      s"${viewDef.externalId}-${viewDef.version}",
      FlexibleDataModelRelationFactory.ResourceType)
}
