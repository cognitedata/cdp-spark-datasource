package cognite.spark.v1

import cognite.spark.compiletime.macros.SparkSchemaHelper.structType
import org.apache.spark.sql.types.StructType

trait InsertSchema {
  val insertSchema: StructType
}

trait UpsertSchema {
  val upsertSchema: StructType
}

trait UpdateSchema {
  val updateSchema: StructType
}

trait UpdateSchemaFromUpsertSchema extends UpsertSchema with UpdateSchema {
  override val updateSchema: StructType = upsertSchema
}

trait ReadSchema {
  val readSchema: StructType
}

trait DeleteSchema {
  val deleteSchema: StructType
}

trait DeleteWithIdSchema extends DeleteSchema {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val deleteSchema: StructType = structType[DeleteByInternalId]()
}

trait DeleteWithExternalIdSchema extends DeleteSchema {
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val deleteSchema: StructType = structType[DeleteByExternalId]()
}

final case class DeleteByInternalId(
    id: Long
)

final case class DeleteByExternalId(
    externalId: String
)
