package cognite.spark.v1.wdl

import cognite.spark.v1.CdfSparkException
import org.scalatest.{FlatSpec, Matchers}

class WdlModelsTest extends FlatSpec with Matchers {
  it should "get from ingestion name" in {
    val wellSource = WdlModels.fromIngestionSchemaName("NptIngestion")
    wellSource.shortName should be("nptevents")
    wellSource.ingest.map(_.schemaName) should be(Some("NptIngestion"))
  }

  it should "get from short name" in {
    val dm = WdlModels.fromShortName("depthmeasurements")
    dm.shortName should be("depthmeasurements")
    dm.ingest.map(_.schemaName) should be(Some("DepthMeasurementIngestion"))
  }

  it should "get from short name: case insensitive" in {
    val dm = WdlModels.fromShortName("depthMeasurements")
    dm.shortName should be("depthmeasurements")
  }

  it should "get from retrieval name" in {
    val nds = WdlModels.fromRetrievalSchemaName("Nds")
    nds.retrieve.schemaName should be("Nds")
    nds.shortName should be("ndsevents")
  }

  it should "give a good error message if the ingestion schema name doesn't exist" in {
    val error = intercept[CdfSparkException] {
      WdlModels.fromIngestionSchemaName("Npt")
    }
    error.getMessage should include("Invalid schema name: `Npt`. The valid options are")
    error.getMessage should include("NptIngestion")
  }

  it should "give a good error message if the short name doesn't exist" in {
    val error = intercept[CdfSparkException] {
      WdlModels.fromShortName("NptIngestion")
    }
    error.getMessage should include(
      "Invalid well data layer data type: `NptIngestion`. The valid options are")
    error.getMessage should include("nptevents")
  }

  it should "give a good error message if the retrieval schema name doesn't exist" in {
    val error = intercept[CdfSparkException] {
      WdlModels.fromRetrievalSchemaName("NptIngestion")
    }
    error.getMessage should include("Invalid schema name: `NptIngestion`. The valid options are")
    error.getMessage should include("Npt")
  }
}
