package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.NodeDeletionRequest
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstanceCreate}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import com.cognite.sdk.scala.v1.{CogniteInstanceId, File, FileCreate, FileUploadExternalId, FileUploadInstanceId, InstanceId, SpaceCreateDefinition}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.net.MalformedURLException
import scala.collection.immutable._

//This test is built with the assumption that these files are not externally modified, in order to skip recreating them if they already exist

class FileContentRelationTest  extends FlatSpec with Matchers with SparkTest with BeforeAndAfterAll {
  //if the files were externally modified, change this to true
  val shouldCleanup: Boolean = false
  val fileExternalId: String = "fileContentTransformationFile"
  val fileExternalIdWithNestedJson: String = "fileContentTransformationFileNestedJson"
  val fileExternalIdWithConflicts: String = "fileContentTransformationFileConflicts"
  val fileWithoutUploadExternalId: String = "fileWithoutUploadExternalId"
  val fileInstanceSpaceExternalId = "fileInstanceSpace"
  val fileInstanceId: InstanceId = InstanceId(space = fileInstanceSpaceExternalId, externalId = fileExternalId)
  val fileWithoutUploadInstanceId: InstanceId = InstanceId(space = fileInstanceSpaceExternalId, externalId = fileWithoutUploadExternalId)

  override def beforeAll(): Unit = {
    if(shouldCleanup) {
      writeClient.files.deleteByExternalIds(Seq(
        fileExternalId,
        fileExternalIdWithNestedJson,
        fileExternalIdWithConflicts,
        fileWithoutUploadExternalId
      )).unsafeRunSync()
      deleteFile(Right(fileInstanceId)).unsafeRunSync()
      deleteFile(Right(fileWithoutUploadInstanceId)).unsafeRunSync()
    }
    makeFile(Left(fileExternalId)).unsafeRunSync()
    makeFile(Left(fileExternalIdWithNestedJson), None, optionalContent = Some(generateNdjsonDataNested)).unsafeRunSync()
    makeFile(Left(fileExternalIdWithConflicts), None, optionalContent = Some(generateNdjsonDataConflicting)).unsafeRunSync()
    makeFile(Left(fileWithoutUploadExternalId), None, None).unsafeRunSync()

    writeClient.spacesv3.createItems(Seq(SpaceCreateDefinition(fileInstanceSpaceExternalId))).unsafeRunSync()
    makeFile(Right(fileInstanceId)).unsafeRunSync()
    makeFile(Right(fileWithoutUploadInstanceId), None, None).unsafeRunSync()
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


  private def getExistingFile(id: Either[String, InstanceId]): IO[File] = {
    id match {
      case Right(instanceId: InstanceId) => writeClient.files.retrieveByInstanceId(CogniteInstanceId(instanceId))
      case Left(externalId: String) => writeClient.files.retrieveByExternalId(externalId)
    }
  }

  private def deleteFile(id: Either[String, InstanceId]): IO[Unit] = {
    id match {
      case Right(instanceId: InstanceId) => writeClient.instances.delete(Seq(NodeDeletionRequest(instanceId.space, instanceId.externalId))) *> IO.unit
      case Left(externalId: String) => writeClient.files.deleteByExternalId(externalId)
    }
  }

  private def getUploadLink(id: Either[String, InstanceId]): IO[File] = {
    id match {
      case Right(instanceId: InstanceId) => writeClient.files.uploadLink(FileUploadInstanceId(instanceId))
      case Left(externalId: String) => writeClient.files.uploadLink(FileUploadExternalId(externalId))
    }
  }

  private def createFile(id: Either[String, InstanceId], mimeType: Option[String]): IO[Unit] = {
    id match {
      case Right(instanceId: InstanceId) =>
        val toCreate: InstanceCreate = InstanceCreate(
          items = Seq(
            NodeWrite(
              space = instanceId.space,
              externalId = instanceId.externalId,
              Some(Seq(EdgeOrNodeData(ViewReference("cdf_cdm", "CogniteFile", "v1"), None))
              ),
              None
            )),
          None,
          None,
          None,
          None
        )
        writeClient.instances.createItems(toCreate).map(_.headOption.getOrElse(throw new IllegalStateException("could not create file"))) *> IO.unit
      case Left(externalId: String) =>
        val toCreate: FileCreate = FileCreate(
          name = "test file for file content transformation",
          externalId = Some(externalId),
          mimeType = mimeType,
        )
        writeClient.files.create(Seq(toCreate)).map(_.headOption.getOrElse(throw new IllegalStateException("could not upload file")))
    }
  }

  private def makeFile(id: Either[String, InstanceId], mimeType: Option[String] = Some("application/jsonlines"), optionalContent: Option[String] = Some(generateNdjsonData)): IO[Unit]  = {
    val wantUploaded = optionalContent.isDefined
    for {
      existingFile <- getExistingFile(id).attempt

      // delete file if it was created but the actual file upload state doesn't match desired state
      _ <- existingFile match {
        case Right(value) if value.uploaded == wantUploaded =>
          IO.unit
        case Right(_) => deleteFile(id)
        case Left(_) => IO.unit
      }

      processedFile <- existingFile match {
        case Right(value) if value.uploaded == wantUploaded => IO(value)
        case _ =>
          createFile(id, mimeType) *>
            retryWhileIO[File](
              getUploadLink(id).recoverWith(e => throw new RetryException(s"Retry needed for file upload link", e)),
              _.uploadUrl.isEmpty
            )
      }

      _ <- {
        if (!processedFile.uploaded && wantUploaded) {
          val backend = Resource.make(IO(HttpURLConnectionBackend()))(b => IO(b.close()))
          val uploadUrl = processedFile
            .uploadUrl
            .getOrElse(throw new MalformedURLException(s"bad url: '${processedFile.uploadUrl}' uploaded status: ${processedFile.uploaded}"))
          val request = basicRequest
            .put(uri"$uploadUrl")
            .body(optionalContent.get)
          backend.use { backend =>
            IO {
              val response = request.send(backend)
              if (response.code.isSuccess) {
                println(s"NDJSON content uploaded successfully to ${processedFile.uploadUrl}")
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
        List[Any](30, "Alice", null),
        List[Any](25, "Bob", null),
        List[Any](35, "Charlie", null),
        List[Any](35, "Charlie2", "test")
      )
  }

  it should "read from a ndJson file (instanceId version)" in {
    val sourceDf: DataFrame = dataFrameReaderUsingOidc
      .useOIDCWrite
      .option("type", "filecontent")
      .option("instanceSpace", fileInstanceSpaceExternalId)
      .option("instanceExternalId", fileExternalId)
      .option("inferSchema", true)
      .load()
    sourceDf.createOrReplaceTempView("fileContent")
    val result = spark.sqlContext.sql(s"select * from filecontent").collect()
    result.map(_.toSeq.toList) should contain theSameElementsAs
      Array(
        List[Any](30, "Alice", null),
        List[Any](25, "Bob", null),
        List[Any](35, "Charlie", null),
        List[Any](35, "Charlie2", "test")
      )
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

  it should "limit by file size in byte" in {
    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileId = Left(fileExternalId),
      true
    )(spark.sqlContext) {
      override val fileSizeLimitBytes: Long = 100
    }

    val expectedMessage = "File with external id: \"fileContentTransformationFile\" size too big. SizeLimit in bytes: 100"
    val exception = sparkIntercept {
      relation.createDataFrame
    }
    withClue(s"Expected '$expectedMessage' but got: '${exception.getMessage}'") {
      exception.getMessage.contains(expectedMessage) should be(true)
    }
  }

  it should "limit by line size in character" in {
    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileId = Left(fileExternalId),
      true
    )(spark.sqlContext) {
      override val lineSizeLimitCharacters: Int = 5
    }

    val expectedMessage = "Line too long in file with external id: \"fileContentTransformationFile\" SizeLimit in characters: 5, but 47 characters accumulated"
    val exception = sparkIntercept {
      relation.createDataFrame
    }
    withClue(s"Expected '$expectedMessage' but got: '${exception.getMessage}'") {
      exception.getMessage.contains(expectedMessage) should be(true)
    }
  }

  it should "throw if the file was never uploaded" in {
    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileId = Left(fileWithoutUploadExternalId),
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

  it should "throw if the file was never uploaded (instanceId version)" in {
    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileId = Right(fileWithoutUploadInstanceId),
      true
    )(spark.sqlContext)

    val expectedMessage = "Could not read file because no file was uploaded for instance id with externalId: fileWithoutUploadExternalId and space: testSpaceForSparkDatasource"
    val exception = sparkIntercept {
      relation.createDataFrame
    }
    withClue(s"Expected '$expectedMessage' but got: '${exception.getMessage}'") {
      exception.getMessage.contains(expectedMessage) should be(true)
    }
  }

}


