package cognite.spark.performancebench

import org.apache.spark.sql.Row

object Warmup extends SparkUtil {
  def run(): Array[Row] =
    read()
      .option("type", "events")
      .load()
      .collect()
}
