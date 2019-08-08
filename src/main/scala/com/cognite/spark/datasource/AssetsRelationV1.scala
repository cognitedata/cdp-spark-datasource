package com.cognite.spark.datasource

import cats.effect.IO
import com.cognite.sdk.scala.v1.{Asset, AssetCreate, GenericClient}
import com.cognite.sdk.scala.v1.resources.Assets
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.InsertableRelation
import io.circe.generic.auto._
import com.cognite.spark.datasource.SparkSchemaHelper._
import com.softwaremill.sttp.Uri
import com.softwaremill.sttp._
import org.apache.spark.sql.types._

import AssetsRelation.fieldDecoder

class AssetsRelationV1(config: RelationConfig)(val sqlContext: SQLContext)
    extends SdkV1Relation[Asset, Assets[IO], AssetsItem](config, "assets")
    with InsertableRelation {

  override def getFromRowAndCreate(rows: Seq[Row]): IO[Seq[Asset]] = {
    val assetCreates = rows.map { r =>
      val asset = fromRow[AssetCreate](r)
      asset.copy(metadata = filterMetadata(asset.metadata))
    }
    client.assets.create(assetCreates)
  }

  override def schema: StructType = structType[Asset]

  override def toRow(a: Asset): Row = asRow(a)

  override def clientToResource(client: GenericClient[IO, Nothing]): Assets[IO] =
    client.assets

  override def listUrl(version: String): Uri =
    uri"${config.baseUrl}/api/$version/projects/${config.project}/assets"

  val cursorsUrl = uri"${listUrl("0.6")}/cursors"

  override def cursors(): Iterator[(Option[String], Option[Int])] =
    CursorsCursorIterator(cursorsUrl.param("divisions", config.partitions.toString), config)
}
