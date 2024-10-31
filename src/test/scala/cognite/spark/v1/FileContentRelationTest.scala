package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.FileCreate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.net.MalformedURLException

class FileContentRelationTest  extends FlatSpec with Matchers with SparkTest with BeforeAndAfterAll {
  val fileExternalId: String = "fileContentTransformationFile"
  val fileExternalIdWithNestedJson: String = "fileContentTransformationFileNestedJson"
  val fileExternalIdWithConflicts: String = "fileContentTransformationFileConflicts"
  val fileWithWrongMimeTypeExternalId: String = "fileContentTransformationFileWrongMimeType"
  val fileWithoutUploadExternalId: String = "fileWithoutUploadExternalId"

  override def beforeAll(): Unit = {
    makeFile(fileExternalId).unsafeRunSync()
    makeFile(fileExternalIdWithNestedJson, None, generateNdjsonDataNested).unsafeRunSync()
    makeFile(fileExternalIdWithConflicts, None, generateNdjsonDataConflicting).unsafeRunSync()

    makeFile(fileWithWrongMimeTypeExternalId, Some("application/json")).unsafeRunSync()//bad mimetype
  }

//  uncomment for cleanups
  override def afterAll(): Unit = {
    writeClient.files.deleteByExternalIds(Seq(fileExternalId)).unsafeRunSync()
  }

  val generateNdjsonData: String = {
    val jsonObjects = List(
      """{"name": "Alice", "age": 30}""",
      """{"name": "Bob", "age": 25}""",
      """{"name": "Charlie", "age": 35}""",
      """{"name": "Charlie2", "age": 35, "test": "test"}"""
    )
    jsonObjects.mkString("\n")
  }

  val generateNdjsonDataNested: String = {
    val jsonObjects = List(
      """{"name": "Alice", "age": 30}""",
      """{"name": "Bob", "age": 25}""",
      """{"name": "Charlie", "age": 35}""",
      """{"name": "Charlie2", "age": 35, "test": [{"key": "value", "key2": "value2"}, {"key": "value"}]}"""
    )
    jsonObjects.mkString("\n")
  }

  val generateNdjsonDataConflicting: String = {
    val jsonObjects = List(
      """{"name": "Alice", "age": 30}""",
      """{"name": "Bob", "age": 25}""",
      """{"name": "Charlie", "age": 35, "test": "content"}""",
      """{"name": "Charlie2", "age": 35, "test": [{"key": "value", "key2": "value2"}, {"key": "value"}]}"""
    )
    jsonObjects.mkString("\n")
  }

  def makeFile(externalId: String, mimeType: Option[String] = Some("application/jsonlines"), content: String = generateNdjsonData): IO[Unit]  = {
    val toCreate: FileCreate = FileCreate(
      name = "test file for file content transformation",
      externalId = Some(externalId),
      mimeType = mimeType,
    )
    for {
      existingFile <- writeClient.files.retrieveByExternalId(externalId).attempt

      // delete file if it was created but the actual file wasn't uploaded so we can get an uploadUrl
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
          val request = basicRequest.put(uri"${file.uploadUrl.getOrElse(throw new MalformedURLException("bad url"))}")
            .body(content)
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
      .option("inferSchema", true)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    val result = spark.sqlContext.sql(s"select * from filecontent").collect()
    result.map(_.toSeq.toList) should contain theSameElementsAs
      Array(
        List(30, "Alice", null),
        List(25, "Bob", null),
        List(35, "Charlie", null),
        List(35, "Charlie2", "test")
      )
  }

  it should "fail with a sensible error if the mimetype is wrong" in {
    val exception = sparkIntercept {
      val sourceDf: DataFrame = dataFrameReaderUsingOidc
        .useOIDCWrite
        .option("type", "filecontent")
        .option("externalId", fileWithWrongMimeTypeExternalId)
        .load()
      sourceDf.createOrReplaceTempView("fileContent")
      spark.sqlContext.sql(s"select * from filecontent").collect()
    }
    assert(exception.getMessage.contains("Wrong mimetype. Expects application/jsonlines"))
  }

  it should "infer the schema" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("inferSchema", value = true)
      .option("externalId", fileExternalId)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    sourceDf.schema.fields should contain allElementsOf Seq(
      StructField("name", StringType, nullable = true),
      StructField("test", StringType, nullable = true),
      StructField("age", LongType, nullable = true)
    )
  }

  it should "infer the schema correctly with nested json" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("inferSchema", value = true)
      .option("externalId", fileExternalIdWithNestedJson)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    sourceDf.schema.fields should contain allElementsOf Seq(
      StructField("name", StringType, nullable = true),
      StructField("test",
        ArrayType(
          StructType(
            Seq(
              StructField("key",StringType, nullable = true),
              StructField("key2",StringType, nullable = true)
            )
          ),
          containsNull = true
        ),
        nullable = true
      ),
      StructField("age", LongType, nullable = true)
    )
  }

  it should "infer the schema correctly with conflicting types" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("inferSchema", value = true)
      .option("externalId", fileExternalIdWithConflicts)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    sourceDf.schema.fields should contain allElementsOf Seq(
      StructField("name", StringType, nullable = true),
      StructField("test", StringType, nullable = true),
      StructField("age", LongType, nullable = true)
    )
  }


  it should "select specific columns" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("externalId", fileExternalId)
      .option("inferSchema", true)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    val result = spark.sqlContext.sql(s"select name, test from filecontent").collect()
    result.map(_.toSeq.toList) should contain theSameElementsAs
      Seq(
        List("Alice", null),
        List("Bob", null),
        List("Charlie", null),
        List("Charlie2", "test")
      )
  }

  it should "not infer schema if not asked to" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("externalId", fileExternalId)
      .option("inferSchema", false)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    val result = spark.sqlContext.sql(s"select * from filecontent").collect()

    sourceDf.schema.fields should contain only StructField("value", StringType, nullable = true)

    result.map(_.toSeq.toList) should contain theSameElementsAs Array(
      List("""{"name": "Alice", "age": 30}"""),
      List("""{"name": "Bob", "age": 25}"""),
      List("""{"name": "Charlie", "age": 35}"""),
      List("""{"name": "Charlie2", "age": 35, "test": "test"}""")
    )
  }

  it should "get size from endpoint and check for it" in {
    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileExternalId = fileExternalId,
      true
    )(spark.sqlContext) {
      override val sizeLimit: Long = 100
    }

    val expectedMessage = "File size too big. Size: 134 SizeLimit: 100"
    val exception = sparkIntercept {
      relation.createDataFrame
    }
    withClue(s"Expected '$expectedMessage' but got: '${exception.getMessage}'") {
      exception.getMessage.contains(expectedMessage) should be(true)
    }
  }

  it should "throw if the file was never uploaded" in {

    val toCreate: FileCreate = FileCreate(
      name = "test file for file content transformation",
      externalId = Some(fileWithoutUploadExternalId),
      mimeType = None,
    )
    for {
      existingFile <- writeClient.files.retrieveByExternalId(fileWithoutUploadExternalId).attempt
      _ <- existingFile match {
        case Right(value) if value.uploaded => IO.pure(value)
        case _ =>
          writeClient.files.create(Seq(toCreate)).map(_.headOption.getOrElse(throw new IllegalStateException("could not upload file")))
      }
    } yield ()

    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileExternalId = fileWithoutUploadExternalId,
      true
    )(spark.sqlContext)

    val expectedMessage = "Could not read file because no file was uploaded for externalId: fileWithoutUploadExternalId"
    val exception = sparkIntercept {
      relation.createDataFrame
    }
    withClue(s"Expected '$expectedMessage' but got: '${exception.getMessage}'") {
      exception.getMessage.contains(expectedMessage) should be(true)
    }
  }
}


