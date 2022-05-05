package cognite.spark.v1.udf

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cognite.spark.v1.Constants.{DefaultBaseUrl, DefaultMaxRetries, DefaultMaxRetryDelaySeconds}
import cognite.spark.v1.udf.CogniteUdfs.{
  callFunctionAndGetResult,
  callFunctionByExternalId,
  callFunctionByName
}
import cognite.spark.v1.{CdfSparkException, CdpConnector, Constants}
import com.cognite.sdk.scala.common.ApiKeyAuth
import com.cognite.sdk.scala.v1.{FunctionCall, GenericClient}
import io.circe.{Json, JsonObject, parser}
import org.apache.spark.sql.SparkSession
import sttp.client3.SttpBackend

class CogniteUdfs(sparkSession: SparkSession) {
  def initializeUdfs(apiKey: ApiKeyAuth, baseUrl: String = DefaultBaseUrl)(
      implicit ioRuntime: IORuntime): Unit = {
    sparkSession.udf.register(
      "cdf_function",
      (functionId: Long, data: String) =>
        callFunctionAndGetResult(
          functionId,
          data,
          apiKey,
          baseUrl
        ).unsafeRunSync()
    )
    sparkSession.udf.register(
      "cdf_function_external_id",
      (externalId: String, data: String) =>
        callFunctionByExternalId(
          externalId,
          data,
          apiKey,
          baseUrl
        ).unsafeRunSync()
    )
    sparkSession.udf.register(
      "cdf_function_name",
      (name: String, data: String) =>
        callFunctionByName(
          name,
          data,
          apiKey,
          baseUrl
        ).unsafeRunSync()
    )
  }
}

object CogniteUdfs {
  @transient implicit lazy val backend: SttpBackend[IO, Any] =
    CdpConnector.retryingSttpBackend(DefaultMaxRetries, DefaultMaxRetryDelaySeconds)

  private def getFunctionResult(client: GenericClient[IO], functionId: Long, result: FunctionCall)(
      implicit ioRuntime: IORuntime): IO[Json] = {
    var res = result
    var i = 0
    while (res.status.isDefined && res.status.get == "Running" && i < 30) {
      Thread.sleep(500)
      i += 1
      res = client
        .functionCalls(functionId)
        .retrieveById(res.id.get)
        .unsafeRunSync()
    }

    if (res.status.isDefined && res.status.get == "Completed") {
      for {
        functionCallResponse <- client
          .functionCalls(functionId)
          .retrieveResponse(res.id.getOrElse(0L))
      } yield functionCallResponse.response.getOrElse(Json.fromJsonObject(JsonObject.empty))
    } else {
      throw new CdfSparkException("CDF function call failed. Call = " + res.toString + ".")
    }
  }

  private def callFunctionAndGetResult(
      functionId: Long,
      data: String,
      apiKeyAuth: ApiKeyAuth,
      baseUrl: String)(implicit ioRuntime: IORuntime): IO[String] = {
    val jsonData = parser.parse(data) match {
      case Right(value) => value
      case Left(_) => Json.fromJsonObject(JsonObject.empty)
    }

    for {
      client <- GenericClient.forAuth[IO](
        Constants.SparkDatasourceVersion,
        apiKeyAuth,
        baseUrl,
        apiVersion = Some("playground"))
      call <- client
        .functionCalls(functionId)
        .callFunction(Json.fromJsonObject(JsonObject(("data", jsonData))))
      res <- getFunctionResult(client, functionId, call)
    } yield res.toString
  }

  private def callFunctionByExternalId(
      externalId: String,
      data: String,
      apiKeyAuth: ApiKeyAuth,
      baseUrl: String)(implicit ioRuntime: IORuntime) = {
    val funcIO = for {
      client <- GenericClient.forAuth[IO](
        Constants.SparkDatasourceVersion,
        apiKeyAuth,
        baseUrl,
        apiVersion = Some("playground"))
      func <- client.functions.retrieveByExternalId(externalId)
    } yield func

    val func = funcIO.unsafeRunSync()
    callFunctionAndGetResult(func.id.get, data, apiKeyAuth, baseUrl)
  }

  private def callFunctionByName(name: String, data: String, apiKeyAuth: ApiKeyAuth, baseUrl: String)(
      implicit ioRuntime: IORuntime) = {
    val functionsIO = for {
      client <- GenericClient.forAuth[IO](
        Constants.SparkDatasourceVersion,
        apiKeyAuth,
        baseUrl,
        apiVersion = Some("playground"))
      functions <- client.functions.read()
    } yield functions

    val functionOption = functionsIO.unsafeRunSync().items.find(func => func.name == name)
    functionOption match {
      case Some(value) => callFunctionAndGetResult(value.id.get, data, apiKeyAuth, baseUrl)
      case None => IO.pure("")
    }
  }
}
