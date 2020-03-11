package cognite.spark.performancebench

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}

trait SparkUtil {
  val spark = SparkUtil.spark
  val cogniteApiKey = sys.env.getOrElse("COGNITE_API_KEY", throw new Exception("'COGNITE_API_KEY' is not set"))

  def read(): DataFrameReader =
    spark.read
      .format("cognite.spark.v1")
      .option("apiKey", cogniteApiKey)

  def write(df: DataFrame): DataFrameWriter[Row] =
    df.write
      .format("cognite.spark.v1")
      .option("apiKey", cogniteApiKey)
}

object SparkUtil {
  lazy val spark = SparkSession.builder
    .appName("CDF Spark Performance Benchmark")
    .master("local[*]")
    .getOrCreate()
}
