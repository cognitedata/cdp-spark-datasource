package cognite.spark.v1

import cats.effect.{IO, Resource}
import com.cognite.sdk.scala.v1.{File, FileCreate, Label, LabelCreate}
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

class FilesRelationTest extends FlatSpec with Matchers with ParallelTestExecution with SparkTest {
  val testSource = s"FilesRelationTest-${shortRandomString()}"

  private def makeDestinationDf(): Resource[IO, (DataFrame, String, String)] = {
    val targetView = s"destinationFile_${shortRandomString()}"
    val metricsPrefix = s"test.${shortRandomString()}"
    Resource.make {
      IO.blocking {
        val destinationDf: DataFrame = spark.read
          .format(DefaultSource.sparkFormatString)
          .useOIDCWrite
          .option("type", "files")
          .option("collectMetrics", "true")
          .option("metricsPrefix", metricsPrefix)
          .load()
        destinationDf.createOrReplaceTempView(targetView)
        (destinationDf, targetView, metricsPrefix)
      }
    } {
      case (_, targetView, _) =>
        IO.blocking {
          spark
            .sql(s"""select * from ${targetView} where source = '${testSource}'""")
            .write
            .format(DefaultSource.sparkFormatString)
            .useOIDCWrite
            .option("type", "files")
            .option("onconflict", "delete")
            .save()
        }
    }
  }

  private def runIOTest[A](test: Resource[IO, A]): Unit =
    test.use(_ => IO.unit).unsafeRunSync()(cats.effect.unsafe.implicits.global)

  private def makeLabels(externalIds: Seq[String]): Resource[IO, Seq[Label]] =
    Resource.make {
      writeClient.labels.create(
        externalIds.map(
          externalId =>
            LabelCreate(
              description = Some("cdp-spark-connector FilesRelationTest"),
              name = "for tests",
              externalId = externalId,
              dataSetId = None)))
    } { _ =>
      writeClient.labels.deleteByExternalIds(externalIds)
    }

  private def makeFiles(toCreate: Seq[FileCreate]): Resource[IO, Seq[File]] = {
    assert(toCreate.forall(_.source.isEmpty), "file.source is reserved for test setup")
    Resource
      .make {
        writeClient.files.create(toCreate.map(_.copy(source = Some(testSource))))
      } { createdFiles =>
        writeClient.files.deleteByIds(createdFiles.map(_.id))
      }
      .evalTap(createdFiles =>
        IO.delay({
          assert(createdFiles.length == toCreate.length, "all files should be created")
          assert(createdFiles.forall(_.source.contains(testSource)), "all created files have source")
        }))
  }

  private def getDfReader(): DataFrameReader =
    spark.read
      .format(DefaultSource.sparkFormatString)
      .option("type", "files")
      .useOIDCWrite

  "FilesRelation" should "read files" taggedAs ReadTest in {
    runIOTest(for {
      _ <- makeFiles(Seq.fill(4)(FileCreate(name = "file")))
      sourceView = s"sourceFiles_${shortRandomString()}"
      sourceDf = getDfReader().load()
      _ = sourceDf.createOrReplaceTempView(sourceView)
      res <- Resource.eval(retryWhileIO[Array[Row]](IO.blocking {
        spark.sqlContext
          .sql(s"select * from ${sourceView} where source = '${testSource}'")
          .collect()
      }, _.length < 4))
      _ = assert(res.length == 4)
    } yield ())
  }

  it should "respect the limit option" taggedAs ReadTest in {
    runIOTest(for {
      _ <- makeFiles(Seq.fill(10)(FileCreate(name = "file")))
      res <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            val df = getDfReader()
              .option("limitPerPartition", "5")
              .option("partitions", "1")
              .load()
            val view = s"files_${shortRandomString()}"
            df.createTempView(view)
            spark.sqlContext
              .sql(s"select * from ${view} where source = '$testSource'")
              .collect()
          },
          _.length < 5
        ))
      _ = assert(res.length == 5)
    } yield ())
  }

  it should "use cursors when necessary" taggedAs ReadTest in {
    runIOTest(for {
      _ <- makeFiles(Seq.fill(10)(FileCreate(name = "file")))
      res <- Resource.eval(
        retryWhileIO[Array[Row]](
          IO.blocking {
            val df = getDfReader()
              .option("batchSize", "2")
              .load()
            val view = s"files_${shortRandomString()}"
            df.createTempView(view)
            spark.sqlContext
              .sql(s"select * from ${view} where source = '$testSource'")
              .collect()
          },
          _.length < 10
        ))
      _ = assert(res.length == 10)
    } yield ())
  }

  it should "support creating files using insertInto" taggedAs WriteTest in {
    runIOTest(for {
      _ <- makeLabels(Seq(s"test-label-1-${testSource}", s"test-label-2-${testSource}"))
      (_, targetView, _) <- makeDestinationDf()
      sourceDf = dataFrameReaderUsingOidc
        .option("type", "files")
        .load()
      _ = spark
        .sql(s"""
             |select "name-$testSource" as name,
             |null as id,
             |null as directory,
             |'$testSource' as source,
             |'externalId-$testSource' as externalId,
             |null as mimeType,
             |null as metadata,
             |null as assetIds,
             |null as datasetId,
             |null as sourceCreatedTime,
             |null as sourceModifiedTime,
             |null as securityCategories,
             |array('test-label-1-${testSource}', 'test-label-2-${testSource}') as labels,
             |null as uploaded,
             |null as createdTime,
             |null as lastUpdatedTime,
             |null as uploadedTime,
             |null as uploadUrl
     """.stripMargin)
        .select(sourceDf.columns.map(col).toIndexedSeq: _*)
        .write
        .insertInto(targetView)
      rows = retryWhile[Array[Row]](
        spark.sql(s"select * from $targetView where source = '$testSource'").collect(),
        rows => rows.length < 1)
      _ = assert(rows.length == 1)
    } yield ())
  }

  it should "support updates using id and externalId" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, metricsPrefix) <- makeDestinationDf()
      _ = spark
        .sql(s"""
             |select "name-$testSource" as name,
             |null as id,
             |'$testSource' as source,
             |'externalId-$testSource' as externalId
     """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()
      rows = retryWhile[Array[Row]](
        spark.sql(s"""select * from ${targetView} where source = "$testSource"""").collect(),
        rows => rows.length < 1)
      _ = assert(rows.length == 1)
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "files") == 1)
      id <- Resource.eval(writeClient.files.retrieveByExternalId(s"externalId-$testSource")).map(_.id)
      //Update using id
      _ = spark
        .sql(s"""
            |select ${id.toString} as id,
            |'$testSource' as source,
            |'updatedById-externalId-$testSource' as externalId
      """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "files") == 1)
      _ = assert(getNumberOfRowsUpdated(metricsPrefix, "files") == 1)
      updatedById = retryWhile[Array[Row]](
        spark
          .sql(s"select * from ${targetView} where externalId = 'updatedById-externalId-$testSource'")
          .collect(),
        df => df.length < 1)
      _ = assert(updatedById.length == 1)

      //Update using externalId
      _ = spark
        .sql(s"""
                    |select 'updatedById-externalId-$testSource' as externalId,
                    |'updatedByExternalId-$testSource' as source
        """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("onconflict", "update")
        .option("collectMetrics", "true")
        .option("metricsPrefix", metricsPrefix)
        .save()
      _ = assert(getNumberOfRowsCreated(metricsPrefix, "files") == 1)
      _ = assert(getNumberOfRowsUpdated(metricsPrefix, "files") == 2)
      updatedByExternalId = retryWhile[Array[Row]](
        spark
          .sql(s"select * from ${targetView} where source = 'updatedByExternalId-$testSource'")
          .collect(),
        df => df.length < 1)
      _ = assert(updatedByExternalId.length == 1)
    } yield ())
  }

  it should "support upserts" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      //insert data
      _ = spark
        .sql(s"""
                  |select "upsert-example" as name,
                  |'$testSource' as source,
                  |'externalId-$testSource' as externalId,
                  |null as id,
                  |null as mimeType
      """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("onconflict", "upsert")
        .save()
      id <- Resource.eval(writeClient.files.retrieveByExternalId(s"externalId-$testSource")).map(_.id)
      insertWithUpsertIds = retryWhile[Array[Row]](
        spark
          .sql(s"select * from ${targetView} where source = '$testSource'")
          .collect(),
        df => df.length < 1)
      _ = assert(insertWithUpsertIds.length == 1)

      //update data
      _ = spark
        .sql(s"""
             |select ${id.toString} as id,
             |'text/plain-$testSource' as mimeType,
             |'upserted-$testSource' as source
""".stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("onconflict", "upsert")
        .save()

      //check updated data
      updatedWithUpsert = retryWhile[Array[Row]](
        spark
          .sql(
            s"select * from ${targetView} where mimeType = 'text/plain-$testSource' and source = 'upserted-$testSource'")
          .collect(),
        df => df.length < 1)
      _ = assert(updatedWithUpsert.length == 1)

      //original data doesn't exist
      updated = retryWhile[Array[Row]](
        spark
          .sql(s"select * from ${targetView} where source = '$testSource'")
          .collect(),
        df => df.length > 0)
      _ = assert(updated.isEmpty)
    } yield ())
  }

  it should "support deletes" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      _ = spark
        .sql(s"""
                |select "name-$testSource" as name,
                |null as id,
                |'$testSource' as source,
                |'externalId-$testSource' as externalId
    """.stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .save()

      rows = retryWhile[Array[Row]](
        spark.sql(s"select id from ${targetView} where source = '$testSource'").collect(),
        rows => rows.length < 1)
      _ = assert(rows.length == 1)

      //Delete using id
      _ = spark
        .sql(s"select ${rows.head.getLong(0)} as id")
        .write
        .format(DefaultSource.sparkFormatString)
        .useOIDCWrite
        .option("type", "files")
        .option("onconflict", "delete")
        .save()

      idsAfterDelete = retryWhile[Array[Row]](
        spark
          .sql(s"select id from ${targetView} where source = '$testSource'")
          .collect(),
        df => df.length > 0)
      _ = assert(idsAfterDelete.isEmpty)
    } yield ())
  }

  it should "correctly have insert < read and upsert < read schema hierarchy" in {
    val filesInsert = FilesInsertSchema("test")
    filesInsert.transformInto[FilesReadSchema]

    val filesUpsert = FilesUpsertSchema()
    filesUpsert.into[FilesReadSchema].withFieldComputed(_.id, eu => eu.id.getOrElse(0L))
  }

  it should "support pushdown filters on labels" taggedAs WriteTest in {
    runIOTest(for {
      (_, targetView, _) <- makeDestinationDf()
      _ <- makeLabels(Seq(s"test-label-${testSource}"))
      _ = spark
        .sql(s"""select '${testSource}-externalId' as externalId,
                |null as id,
                |null as directory,
                |'name-$testSource' as name,
                |'${testSource}' as source,
                |array('test-label-${testSource}') as labels""".stripMargin)
        .write
        .format(DefaultSource.sparkFormatString)
        .option("type", "files")
        .useOIDCWrite
        .save()
      res1 = retryWhile[Array[Row]](
        spark.sql(s"""select * from ${targetView}
             |where labels = array('test-label-${testSource}')
             |and source='${testSource}'""".stripMargin).collect(),
        df => df.length != 1
      )
      _ = res1.length shouldBe 1

      res2 = retryWhile[Array[Row]](
        spark.sql(s"""select * from ${targetView}
             |where labels in(array('test-label-${testSource}'), NULL)
             |and source='${testSource}'""".stripMargin).collect(),
        df => df.length != 1
      )
      _ = res2.length shouldBe 1

      res3 = retryWhile[Array[Row]](
        spark.sql(s"""select * from ${targetView}
             |where labels in(array('nonExistingLabel'), NULL)
             |and source='${testSource}'""".stripMargin).collect(),
        df => df.length != 0
      )
      _ = res3.length shouldBe 0
    } yield ())
  }
}
