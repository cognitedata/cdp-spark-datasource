package cognite.spark.v1

import cognite.spark.compiletime.macros.SparkSchemaHelper.structType
import org.apache.spark.sql.types.StructType

/**
  * Schema traits here aren't strict or currently compile-time checked, but they should
  * indicate a schema compatible with what a relation expects or produces
  *
  * ReadSchema: for read operations
  *
  * DeleteSchema: for deletes
  *
  * {Insert,Upsert,Update}Schema: for writes, differ by conflict resolution mode
  * - upsert: fully overwrites destination items if they exist already
  * - insert: should fail if item already exists in destination. This is fragile as operation
  *           retries on network errors may be doomed to fail
  * - update: partially overwrites existing destination items, keeps properties that aren't part
  *           of update schema (or from update row) intact
  */
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

// TODO: this isn't applied to some relations that have read support
trait ReadSchema {
  val readSchema: StructType
}

trait DeleteSchema {
  val deleteSchema: StructType
}

trait DeleteWithIdSchema extends DeleteSchema {
  import org.apache.spark.sql.types._
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val deleteSchema: StructType = structType[DeleteByInternalId]()
}

trait DeleteWithExternalIdSchema extends DeleteSchema {
  import org.apache.spark.sql.types._
  import cognite.spark.compiletime.macros.StructTypeEncoderMacro._

  override val deleteSchema: StructType = structType[DeleteByExternalId]()
}

final case class DeleteByInternalId(
    id: Long
)

final case class DeleteByExternalId(
    externalId: String
)
