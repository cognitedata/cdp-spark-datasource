package cognite.spark.v1

import cognite.spark.v1.udf.CogniteUdfs
import cats.effect.IO
import cognite.spark.v1.Constants.{DefaultBaseUrl, DefaultMaxRetries, DefaultMaxRetryDelaySeconds}
import com.cognite.sdk.scala.v1.GenericClient
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Inspectors, Matchers, ParallelTestExecution}
import sttp.client3.SttpBackend


class FunctionsUDFTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest with Inspectors {

  ignore should "read assets with functionUDF" taggedAs ReadTest in {
    implicit val backend: SttpBackend[IO, Any] = CdpConnector.retryingSttpBackend(DefaultMaxRetries, DefaultMaxRetryDelaySeconds)
    val df = spark.read
      .format("cognite.spark.v1")
      .option("apiKey", readApiKey)
      .option("type", "assets")
      .option("limitPerPartition", "1")
      .option("partitions", "1")
      .option("limit", "10")
      .load()

    new CogniteUdfs(spark).initializeUdfs(writeApiKeyAuth)

    val func = for {
      client <- GenericClient.forAuth[IO](
        Constants.SparkDatasourceVersion,
        writeApiKeyAuth,
        DefaultBaseUrl,
        apiVersion = Some("playground"))
      functions <- client.functions.read()
    } yield functions.items.head
    val functionId = func.unsafeRunSync().id.get


    df.createOrReplaceTempView("assetsRead")
    val res: Array[Row] = spark
      .sql(s"""select cdf_function($functionId, \"{}\") as functionResult from assetsRead""")
      .collect()

    forAll (res) { row: Row => row.getString(0) shouldEqual "42" }

    assert(res.length == 1)
  }
}
