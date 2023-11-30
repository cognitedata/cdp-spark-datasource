package cognite.spark.v1

import cats.data.Kleisli
import cats.effect.IO
import natchez.Trace
import org.apache.spark.sql.{DataFrame, Row}

final case class DataFrameF(df: DataFrame) {
  def foreachPartition(f: Iterator[Row] => TracedIO[Unit]): TracedIO[Unit] = {
    import CdpConnector.ioRuntime
    Trace[TracedIO].span("foreachPartition")(Kleisli { commonSpan =>
      IO.blocking {
        df.foreachPartition(
          f.andThen(
            op =>
              commonSpan
                .span("partition")
                .use(span => op.run(span))
                .unsafeRunSync()))
      }
    })
  }
}

object SparkF {
  def apply(df: DataFrame): DataFrameF = DataFrameF(df)
}
