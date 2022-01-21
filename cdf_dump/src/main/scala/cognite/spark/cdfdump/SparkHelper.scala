package cognite.spark.cdfdump

import cognite.spark.v1.{CdfSparkException, Constants}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import cats.syntax.all._
import org.apache.spark.sql.functions.col

class SparkHelper(
  writeOptions: Map[String, String],
  readOptions: Map[String, String],
  outDir: String,
  format: String,
  columns: Option[List[String]],
  excludeColumns: List[String],
  filter: Option[String],
  outPartitions: Option[Int],
  maxRetries: Option[Int]
) {
  val baseUrl = sys.env.getOrElse("COGNITE_BASE_URL", Constants.DefaultBaseUrl)
  val project = sys.env.get("COGNITE_PROJECT")
  val auth: Map[String, String] =
    sys.env.get("COGNITE_API_KEY").map(key => Map("apiKey" -> key))
      .orElse(
        (
          sys.env.get("COGNITE_TOKEN_URI").orElse(sys.env.get("COGNITE_TOKEN_URL")),
          sys.env.get("COGNITE_CLIENT_ID"),
          sys.env.get("COGNITE_CLIENT_SECRET")
        ).mapN((url, id, secret) => Map(
          "tokenUri" -> url,
          "clientId" -> id,
          "clientSecret" -> secret,
          "scopes" -> sys.env.getOrElse("COGNITE_SCOPES",
            if (baseUrl.contains("localhost")) {
              throw new Exception(s"Must specify COGNITE_SCOPES variable when using a localhost baseUrl: $baseUrl. Use you real baseUrl/.default")
            } else {
              s"$baseUrl/.default"
            }),
          "project" -> project.getOrElse(throw new Exception("OIDC auth requires COGNITE_PROJECT variable."))
        ))
      ).orElse(
        sys.env.get("COGNITE_BEARER_TOKEN").map(token => Map("bearerToken" -> token))
      ).getOrElse(
      throw new Exception(
        s"Auth not specified correctly." +
          s"Use COGNITE_TOKEN_URL, COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET, COGNITE_PROJECT or COGNITE_API_KEY env variables.")
    )

  val spark = SparkHelper.spark
  spark.sparkContext.setLogLevel("WARN")

  // some magic spell from https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file/27532248#27532248
  private val hadoopConfig = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  def cdfRead(): DataFrameReader = {
    val rdr = spark.read
      .format("cognite.spark.v1")
      .options(auth)
    project.foreach(rdr.option("project", _))
    rdr.option("baseUrl", baseUrl)
    maxRetries.foreach(rdr.option("maxRetries", _))
    rdr
      .option("applicationName", "cdf_dump")
      .option("collectMetrics", "true")
      .options(readOptions)
  }

  def cdfWrite(df: DataFrame): DataFrameWriter[Row] = {
    val wr = df.write
      .format("cognite.spark.v1")
      .options(auth)
    project.foreach(wr.option("project", _))
    baseUrl.foreach(wr.option("baseUrl", _))
    wr.options(writeOptions)
  }

  def save(df: DataFrame, name: String): Unit = {
    val df1 = filter.fold(df)(f => df.filter(f))

    val df2 = columns
      .fold(df1)(x => df1.selectExpr(x: _*))

    val df3 = df2.select(df2.columns.diff(excludeColumns).map(col(_)): _*)

    val df4 = outPartitions.filter(_ => format != "parquet" && format != "orc")
      .fold(df3)(p => df3.repartition(p))

    df4
      .write
      .format(format)
      .options(writeOptions)
      .save(outDir + "/" + name)
  }


  def saveRaw(db: String, table: String): Unit = {
    val d = cdfRead()
      .option("type", "raw")
      .option("database", db)
      .option("table", table)
      .option("inferSchema", "true")
      .load()
    save(d, s"raw/$db/$table")
  }

  def saveClean(t: String): Unit = {
    val d = cdfRead()
      .option("type", t)
      .load()
    save(d, t)
  }
}

object SparkHelper {
  lazy val spark = SparkSession.builder
    .appName("CDF Dump")
    .master("local[*]")
    .getOrCreate()
}
