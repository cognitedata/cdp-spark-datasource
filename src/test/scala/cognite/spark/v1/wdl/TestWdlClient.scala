package cognite.spark.v1.wdl

import cats.effect.IO
import cognite.spark.v1.CdfSparkException
import com.cognite.sdk.scala.v1._
import com.cognite.sdk.scala.v1.GenericClient
import org.apache.spark.sql.types.{DataType, StructType}

class TestWdlClient(val client: GenericClient[IO]) {
  import cognite.spark.v1.CdpConnector._

  def getSchema(name: String): StructType = {
    val schemaAsString = client.wdl.getSchema(name).unsafeRunSync()
    DataType.fromJson(schemaAsString) match {
      case s @ StructType(_) => s
      case _ => throw new CdfSparkException("Failed to decode well-data-layer schema into StructType")
    }
  }

  def deleteAll(): Unit = {
    val result = for {
      sources <- client.wdl.sources.list()
      _ <- if (sources.nonEmpty) {
        client.wdl.sources.deleteRecursive(sources)
      } else {
        IO.unit
      }
    } yield ()
    result.unsafeRunSync()
  }

  case class MiniSetup(
      source: Source,
      well: Well,
      wellSource: WellSource,
      wellbores: Seq[Wellbore],
      wellboreSource: Seq[WellboreSource],
  )

  def miniSetup(): MiniSetup = {
    val wellSource = WellSource(
      matchingId = Some("w1"),
      source = AssetSource(assetExternalId = "A:w1", sourceName = "A"),
      name = "w1",
      wellhead = Some(Wellhead(x = 0.0, y = 60.0, crs = "EPSG:4326"))
    )
    val wellboreSources = Seq(
      WellboreSource(
        matchingId = Some("wb1"),
        name = "wb1",
        source = AssetSource(assetExternalId = "A:wb1", sourceName = "A"),
        datum = Some(Datum(value = 50.0, unit = "meter", reference = "KB")),
        wellAssetExternalId = "A:w1",
      ),
      WellboreSource(
        matchingId = Some("wb2"),
        name = "wb1",
        source = AssetSource(assetExternalId = "A:wb2", sourceName = "A"),
        datum = Some(Datum(value = 50.0, unit = "meter", reference = "KB")),
        wellAssetExternalId = "A:w1",
      ),
    )
    val result = for {
      sources <- client.wdl.sources.create(Seq(Source("A")))
      source = sources.head
      _ <- client.wdl.wells.setMergeRules(WellMergeRules(Seq("A")))
      _ <- client.wdl.wellbores.setMergeRules(WellboreMergeRules(Seq("A")))
      wells <- client.wdl.wells.create(Seq(wellSource))
      well = wells.head
      wellbores <- client.wdl.wellbores.create(wellboreSources)
    } yield
      MiniSetup(
        source = source,
        well = well,
        wellSource = wellSource,
        wellbores = wellbores,
        wellboreSource = wellboreSources
      )

    result.unsafeRunSync()
  }

  def initTestWells(): Seq[Well] = {
    val items = Seq(
      WellSource(
        name = "34/10-8",
        uniqueWellIdentifier = Some("34/10-8"),
        waterDepth = Some(Distance(value = 100.0, unit = "meter")),
        wellhead = Some(
          Wellhead(
            x = 457008.04,
            y = 6781760.88,
            crs = "EPSG:23031"
          )),
        source = AssetSource(assetExternalId = "asset:34/10-8", sourceName = "EDM"),
        description = Some("this is a test well ingestion"),
        country = Some("Norway"),
        quadrant = Some("8"),
        spudDate = Some(s"2017-05-17"),
        block = Some("34/10"),
        field = Some("field"),
        operator = Some("Op1"),
        wellType = Some("production"),
        license = Some("MIT"),
        region = Some("MyRegion"),
      )
    )
    client.wdl.wells.create(items).unsafeRunSync()
  }
}
