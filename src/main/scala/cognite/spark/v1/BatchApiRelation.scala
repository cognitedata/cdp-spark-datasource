package cognite.spark.v1

import com.cognite.sdk.scala.v1.fdm.common.filters.FilterDefinition
import com.cognite.sdk.scala.v1.fdm.instances.{InstanceDefinition, InstanceType, TypePropertyDefinition}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference

import java.util.UUID

class BatchApiRelation {}

final case class BeginRequest(
    includeTyping: Boolean,
    sources: Seq[SourceSelectorWithoutTargetUnits],
    instanceType: InstanceType,
    filter: Option[FilterDefinition] = None,
)

final case class SourceSelectorWithoutTargetUnits(
    source: ViewReference,
)

final case class BeginResponse(
    typing: Option[Map[String, Map[String, Map[String, TypePropertyDefinition]]]] = None,
    statusId: UUID
)

sealed trait SyncStatus { def value: String }

object SyncStatus {
  case object Pending extends SyncStatus { val value = "pending" }
  case object Backfilling extends SyncStatus { val value = "backfilling" }
  case object Progressing extends SyncStatus { val value = "progressing" }
  case object Ready extends SyncStatus { val value = "ready" }
  case object Failed extends SyncStatus { val value = "failed" }
}

sealed trait PollResponse { def status: SyncStatus }

object PollResponse {
  case object Pending extends PollResponse { val status: SyncStatus = SyncStatus.Pending }
  case object Backfilling extends PollResponse { val status: SyncStatus = SyncStatus.Backfilling }
  case object Progressing extends PollResponse { val status: SyncStatus = SyncStatus.Progressing }
  case object Failed extends PollResponse { val status: SyncStatus = SyncStatus.Failed }
  final case class Ready(readyId: UUID) extends PollResponse {
    val status: SyncStatus = SyncStatus.Ready
  }
}

final case class ReadCursorsRequest(readyId: UUID)
final case class ReadCursorsResponse(readCursors: Seq[String])

final case class ReadRequest(readCursor: String)

final case class ReadResponse(
    items: Seq[InstanceDefinition],
    nextReadCursor: Option[String]
)
