package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.FileCreate
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.net.MalformedURLException

class FileContentRelationTest  extends FlatSpec with Matchers with SparkTest with BeforeAndAfterAll {
  val fileExternalId: String = "fileContentTransformationFile"
  val fileWithWrongMimeTypeExternalId: String = "fileContentTransformationFileWrongMimeType"

  override def beforeAll(): Unit = {
    makeFile(fileExternalId).unsafeRunSync()
    makeFile(fileWithWrongMimeTypeExternalId, Some("application/json")).unsafeRunSync()//bad mimetype
  }

//  uncomment for cleanups
  override def afterAll(): Unit = {
    writeClient.files.deleteByExternalIds(Seq(fileExternalId)).unsafeRunSync()
  }

  def generateNdjsonData: String = {
    val jsonObjects = List(
      """{"name": "Alice", "age": 30}""",
      """{"name": "Bob", "age": 25}""",
      """{"name": "Charlie", "age": 35}"""
    )
    jsonObjects.mkString("\n")
  }

  def makeFile(externalId: String, mimeType: Option[String] = Some("application/jsonlines")): IO[Unit]  = {
    val toCreate: FileCreate = FileCreate(
      name = "test file for file content transformation",
      externalId = Some(externalId),
      mimeType = mimeType,
    )
    for {
      existingFile <- writeClient.files.retrieveByExternalId(externalId).attempt

      // delete file if it wasn't uploaded so we can get an uploadUrl
      _ <- existingFile match {
        case Right(value) if !value.uploaded => writeClient.files.deleteByExternalIds(Seq(externalId))
        case _ => IO.pure(())
      }

      file <- existingFile match {
        case Right(value) if value.uploaded => IO.pure(value)
        case _ =>
          writeClient.files.create(Seq(toCreate)).map(_.headOption.getOrElse(throw new IllegalStateException("could not upload file")))
      }

      _ <- {
        if (!file.uploaded) {
          val backend = Resource.make(IO(HttpURLConnectionBackend()))(b => IO(b.close()))
          val request = basicRequest
            .post(uri"${file.uploadUrl.getOrElse(throw new MalformedURLException("bad url"))}")
            .body(generateNdjsonData)
          backend.use { backend =>
            IO {
              val response = request.send(backend)
              if (response.code.isSuccess) {
                println(s"NDJSON content uploaded successfully to ${file.uploadUrl}")
              } else {
                throw new Exception(s"Failed to upload content: ${response.statusText}")
              }
            }
          }
        } else {
          IO.pure(())
        }
      }
    } yield ()
  }

  "file content transformations" should "read from a ndjson file" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("externalId", fileExternalId)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    spark.sqlContext.sql(s"select * from filecontent")
  }

  it should "fail with a sensible error if the mimetype is wrong" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("externalId", fileWithWrongMimeTypeExternalId)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    val exception = sparkIntercept(spark.sqlContext.sql(s"select * from filecontent").collect())
    assert(exception.getCause.getMessage.contains("Wrong mimetype"))
  }

}
