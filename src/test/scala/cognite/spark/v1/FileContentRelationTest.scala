package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.FileCreate
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.net.MalformedURLException

class FileContentRelationTest  extends FlatSpec with Matchers with SparkTest {
  val fileExternalId: String = "fileContentTransformationFile"

  def generateNdjsonData: String = {
    // Generate NDJSON content (newline-delimited JSON strings)
    val jsonObjects = List(
      """{"name": "Alice", "age": 30}""",
      """{"name": "Bob", "age": 25}""",
      """{"name": "Charlie", "age": 35}"""
    )
    jsonObjects.mkString("\n")
  }

  def makeFile: IO[Unit]  = {
    val toCreate: FileCreate = FileCreate(
      name = "test file for file content transformation",
      externalId = Some(fileExternalId),
      mimeType = Some("application/jsonlines")
    )
    for {
      uploadUrl <- writeClient.files.create(Seq(toCreate)).map(_.headOption.getOrElse(throw new IllegalStateException("could not upload file"))).map(_.uploadUrl)
      backend = Resource.make(IO(HttpURLConnectionBackend()))(b => IO(b.close()))
      request = basicRequest
        .post(uri"${uploadUrl.getOrElse(throw new MalformedURLException("bad url"))}")
        .body(generateNdjsonData)
      _ <- backend.use { backend =>
        IO {
          val response = request.send(backend)
          if (response.code.isSuccess) {
            println(s"NDJSON content uploaded successfully to $uploadUrl")
          } else {
            throw new Exception(s"Failed to upload content: ${response.statusText}")
          }
        }
      }
    } yield ()
  }

  //TODO execute once and ensure it's run before the tests


  "file content transformations" should "read from a ndjson file" in {
    makeFile.unsafeRunSync()
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .option("type", "filecontent")
      .option("externalId", fileExternalId)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    spark.sqlContext.sql(s"select * from filecontent")
  }


}
