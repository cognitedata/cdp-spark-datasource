package cognite.spark.v1

import org.apache.spark.Partition

final case class CdfPartition(index: Int) extends Partition
