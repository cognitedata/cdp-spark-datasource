package cognite.spark.v1

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cognite.spark.v1.fdm.utils.FDMTestConstants.spaceExternalId
import com.cognite.sdk.scala.v1.fdm.Utils
import com.cognite.sdk.scala.v1.fdm.instances.InstanceDeletionRequest.NodeDeletionRequest
import com.cognite.sdk.scala.v1.fdm.instances.NodeOrEdgeCreate.NodeWrite
import com.cognite.sdk.scala.v1.fdm.instances.{EdgeOrNodeData, InstanceCreate}
import com.cognite.sdk.scala.v1.fdm.views.ViewReference
import com.cognite.sdk.scala.v1.{CogniteInstanceId, FileCreate, InstanceId}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import scala.collection.immutable._
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
//  override def afterAll(): Unit = {
//    writeClient.files.deleteByExternalIds(Seq(
//      fileExternalId,
//      fileExternalIdWithNestedJson,
//      fileExternalIdWithConflicts,
//      fileWithWrongMimeTypeExternalId,
//      fileWithoutUploadExternalId
//    )).unsafeRunSync()
//  }

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

  def makeFile(instanceId: InstanceId, mimeType: Option[String] = Some("application/jsonlines"), content: String = generateNdjsonData): IO[Unit]  = {
    val cogniteInstanceId: CogniteInstanceId = CogniteInstanceId(instanceId)
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
    for {
      existingFile <- writeClient.files.retrieveByInstanceId(cogniteInstanceId).attempt

      // delete file if it was created but the actual file wasn't uploaded so we can get an uploadUrl
      _ <- existingFile match {
        case Right(value) if !value.uploaded => writeClient.instances.delete(Seq(NodeDeletionRequest(instanceId.space, instanceId.externalId))).attempt
        case _ => IO.pure(())
      }

      fileInstance <- existingFile match {
        case Right(value) if value.uploaded => IO.pure(value)
        case _ =>
          writeClient.instances.createItems(toCreate).map(_.headOption.getOrElse(throw new IllegalStateException("could not create file")))
      }

      file <- writeClient.files.retrieveByInstanceId(cogniteInstanceId)

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
  def makeFileByInstanceId(externalId: String, mimeType: Option[String] = Some("application/jsonlines"), content: String = generateNdjsonData): IO[Unit]  = {
    IO.pure()
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

    val toCreate: FileCreate = FileCreate(
      name = "test file for file content transformation",
      externalId = Some(fileWithoutUploadExternalId),
      mimeType = None,
    )
    val file = for {
      existingFile <- writeClient.files.retrieveByExternalId(fileWithoutUploadExternalId).attempt
      _ <- existingFile match {
        case Right(value) if !value.uploaded => IO.pure(value)
        case _ =>
          writeClient.files.create(Seq(toCreate)).map(_.headOption.getOrElse(throw new IllegalStateException("could not upload file")))
      }
    } yield ()
    file.unsafeRunSync()

    val relation = new FileContentRelation(
      getDefaultConfig(auth = CdfSparkAuth.OAuth2ClientCredentials(credentials = writeCredentials), projectName = OIDCWrite.project, cluster = OIDCWrite.cluster, applicationName = Some("jetfire-test")),
      fileId = Left(fileExternalId),
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


