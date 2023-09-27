package cognite.spark.performancebench

import cognite.spark.v1.CdfSparkException
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}

trait SparkUtil {
  val spark = SparkUtil.spark
  val cogniteAuthOptions = sys.env.getOrElse("SOME_COGNITE_OIDC_VARS", throw new CdfSparkException("TODO: Auth vars aren't set or handled"))

  def read(): DataFrameReader =
    spark.read
      .format("cognite.spark.v1")

  def write(df: DataFrame): DataFrameWriter[Row] =
    df.write
      .format("cognite.spark.v1")
}

object SparkUtil {
  lazy val spark = SparkSession.builder
    .appName("CDF Spark Performance Benchmark")
    .master("local[*]")
    .getOrCreate()
}
