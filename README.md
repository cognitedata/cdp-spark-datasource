# Spark data source for the Cognite Data Fusion API

A [Spark](https://spark.apache.org/) data source for the [Cognite Data Fusion](https://doc.cognitedata.com/).

Supports read and write for raw and clean data types.

Reads and writes are done in parallel using asynchronous calls.
Reads will start only after all cursors have been retrieved, which can take a while
if there are many items.

See instructions below for examples using different resource types.

## Build the project with sbt:

The project runs read-only integration tests against the Open Industrial Data project. Head over to
https://openindustrialdata.com/ to get an API key and store it in the environment variable `TEST_API_KEY_READ`.
To run the write integration tests you'll also need to set the environment variable `TEST_API_KEY_WRITE`
to an API key to a project where you have write access. To run tests against greenfield set the environment
variable `TEST_API_KEY_GREENFIELD` to an API key with read access to the project cdp-spark-datasource-test.

### Setting up
First run `sbt compile` to generate Scala sources for protobuf.

If you have set `TEST_API_KEY_WRITE` run the Python files `scripts/createThreeDData.py` and `scripts/createFilesMetaData.py`
(you'll need to install cognite-sdk-python and set the environment variables `PROJECT` and `TEST_API_KEY_WRITE`).
This will upload a 3D model to your project which is used for testing.

### Running the tests

To run all tests run `sbt test`.

To run groups of tests enter sbt shell mode `sbt>`

To run only the read-only tests run `sbt> testOnly -- -n ReadTest`

To run only the write tests run `sbt> testOnly -- -n WriteTest`

To run only the greenfield tests run `sbt> testOnly -- -n GreenfieldTest`

To run all tests except the write tests run `sbt> testOnly -- -l WriteTest`

To skip the read/write tests in assembly you can add `test in assembly := {}` to build.sbt, or run:

Windows: `sbt "set test in assembly := {}" assembly`

Linux/macos: `sbt 'set test in assembly := {}' assembly`

### Building the .jar

Run `sbt assembly` to create `~/path-to-repo/target/scala-2.11/cdp-spark-datasource-*-jar-with-dependencies.jar`.


## Run it with spark-shell

To run locally you'll need spark-metrics (marked as provided), which can be found at
https://github.com/cognitedata/spark-metrics/releases/tag/v2.4.0-cognite.
Download the jar-file and place under SPARK_HOME/jars.

Get an API-key for the Open Industrial Data project at https://openindustrialdata.com and run the following:

```
$> spark-shell --jars ~/path-to-repo/target/cdp-spark-datasource-*-jar-with-dependencies.jar
Spark context Web UI available at http://IP:4040
Spark context available as 'sc' (master = local[*], app id = local-1513307936323).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/
Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_131)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val apiKey="secret-key-you-have"
scala> val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "assets")
  .load()

df: org.apache.spark.sql.DataFrame = [name: string, parentId: bigint ... 3 more fields]

scala> df.count
res0: Long = 1000
```

## Reading and writing Cognite Data Fusion resource types:

cdp-spark-datasource supports reads from, and writes to, assets, time series, raw tables, data points and events.
You can also read files metadata and 3D-files metadata.

### Common options

Some options are common to all resource types, and can be set with
`spark.read.format("com.cognite.spark.datasource").option("nameOfOption", "value")`.

The common options are:
- `apiKey`: *REQUIRED IF NO BEARER TOKEN* A Cognite Data Fusion [API key](https://doc.cognitedata.com/dev/guides/iam/authentication.html#api-keys) to be used for authorization.
- `bearerToken`: *REQUIRED IF NO API-KEY* A Cognite Data Fusion [token](https://doc.cognitedata.com/dev/guides/iam/authentication.html#tokens) to be used for authorization.
- `type`: *REQUIRED* The Cognite Data Fusion resource type. See below for more information.
- `maxRetries`: The maximum number of retries to be made when a request fails. The default value is 10.
- `limit`: The number of items to fetch for this resource type to create the DataFrame. Note that this is different
from the SQL `SELECT * FROM ... LIMIT 1000` limit. This option specifies the limit for the items to be fetched from
the data fusion, *before* filtering and other transformations are applied to limit the number of results.
- `batchSize`: Maximum number of items to read/write per API call.
- `baseUrl`: Set the prefix to be used for all CDP API calls. The default is https://api.cognitedata.com

### Reading

To read from CDP resource types you need to provide two things:
an API-key and the resource type. To read from a table you should also specify the database name and table name.

### Writing

There are two ways you can use cdp-spark-datasource to write to CDP: using `insertInto` or using the `save` function. 
- `insertInto` - will check that all fields are present and in the correct order, and can be more convenient when
working with Spark SQL tables.
- `save` - will give you control over how to handle potential collisions with existing data,
and allows updating a subset of fields in a row. 

#### Writing with `.insertInto()`

To write to a resource using the insert into pattern you'll need to register a DataFrame that was read from
that resource as a temporary view. You'll need a project where you have write access
and replace `myApiKey` in the examples below.

Your schema will have to match that of the target exactly. A convenient way to ensure
this is to copy the schema from the DataFrame you read into with `sourceDf.select(destinationDf.columns.map(col):_*)`, see time series example.

`.insertInto()` will do upsert, as in updating existing rows and inserting new rows, for events and time series. Events are matched on
`source+sourceId` while time series are matched on `id`.
It will do insert for assets, raw tables and data points, and throw an error if one or more rows already exist.

#### Writing with `.save()`

Writing with `.save()` is currently supported for assets, events and time series.

You'll need to provide an API-key and the event type you'd like to write to. In addition you can specify
the desired behaviour when rows in your Dataframe are present in CDF with the `.option("onconflict", value)`.

The valid options for onconflict are
- `abort` - will try to insert all rows in the Dataframe. An error will be thrown if the resource item already exists and no more rows will be written.
- `update` - will look for all rows in the Dataframe in CDP and try to update them. If one or more rows do not exist no more rows will be updated and an error will be thrown.
Supports partial updates.
- `upsert` - will update rows that already exist, and insert new rows.

See an example for using `.save()` under Events below.

### Assets

https://doc.cognitedata.com/concepts/#assets

```scala
// Read assets from your project into a DataFrame
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
 .option("apiKey", "myApiKey")
 .option("type", "assets")
 .load()

// Register your assets in a temporary view
df.createTempView("assets")

// Create a new asset and write to CDP
// Note that parentId, asset type IDs and asset type field IDs have to exist
val assetColumns = Seq("id", "path", "depth", "name", "parentId", "description",
                   "types", "metadata", "source", "sourceId", "createdTime", "lastupdatedTime")
val someAsset = Seq(
(99L, Seq(0L), 99L, "99-BB-99999", 2231996316030451L, "This is another asset",
Seq(), Map("sourceSystem"->"MySparkJob"), "some source", "some source id", 99L, 99L))
val someAssetDf = someAsset.toDF(assetColumns:_*)

// Write the new asset to CDF, ensuring correct schema by borrowing the schema of the df from CDF
spark
  .sqlContext
  .createDataFrame(someAssetDf.rdd, df.schema)
  .write
  .insertInto("assets")
```

#### Asset types
```scala
// Assets have support for typed metadata for groups of assets such as wells or valves.
// To manually create an asset of an existing type we write to the types field using Scala sequences
val assetColumns = Seq("id", "path", "depth", "name", "parentId", "description",
                   "types", "metadata", "source", "sourceId", "createdTime", "lastupdatedTime")
val someAssetWithAssetType = Seq(
  (
  99L, Seq(0L), 99L, "99-BB-99999", 2231996316030451L, "This is an asset with an asset type",
  Seq((100L, // asset type ID
    "some asset type", // asset type name
    Seq((200L, // asset type field ID
      "some asset type field of type String", // field name
      "String", // field valueType
      "Some value" // field value
    ))
  )),
  Map("sourceSystem"->"MySparkJob"),
  "some source", "some source id",
  99L,
  99L
  )
)

val someAssetWithAssetTypeDf = someAsset.toDF(assetColumns:_*)

// Write to CDF
spark
  .sqlContext
  .createDataFrame(someAssetWithAssetTypeDf.rdd, df.schema)
  .write
  .insertInto("assets")
```

### Time series

https://doc.cognitedata.com/concepts/#time-series

```scala
// Get all the time series from your project
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("type", "timeseries")
  .load()
destinationDf.createTempView("timeseries")

// Read some new time series data from a csv-file
val timeSeriesDf = spark.read.format("csv")
  .option("header", "true")
  .load("timeseries.csv)

// Ensure correct schema by copying the columns in the DataFrame read from the project.
// Note that the time series must already exist in the project before data can be written to it, based on the ´name´ column.
timeSeriesDf.select(destinationDf.columns.map(col):_*)
  .write
  .insertInto("timeseries")
```

### Data points

https://doc.cognitedata.com/concepts/#data-points

Data points are always related to a time series. To read datapoints you will need to filter by a valid time series name,
otherwise an empty DataFrame will be returned. For this reason it is important to be careful when using caching with
this resource type.

One additional option is supported:
- `partitions`: The data source will split the time range into this many partitions (20 by default)
time intervals.

You can also request aggregated data by filtering by aggregation and granularity.

`aggregation`: Numerical data points can be aggregated before they are retrieved from CDP.
This allows for faster queries by reducing the amount of data transferred.
You can aggregate data points by specifying one or more aggregates (e.g. average, minimum, maximum)
as well as the time granularity over which the aggregates should be applied (e.g. "1h" for one hour).
If the aggregate option is NULL, or not set, data points will return the raw time series data.

`granularity`: Aggregates are aligned to the start time modulo the granularity unit.
For example, if you ask for daily average temperatures since monday afternoon last week,
the first aggregated data point will contain averages for monday, the second for tuesday, etc.

```scala
// Get the datapoints from publicdata
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "publicdataApiKey")
  .option("type", "datapoints")
  .load()

// Create the view to enable SQL syntax
df.createTempView("datapoints")

// Read the raw datapoints from the VAL_23-ESDV-92501A-PST:VALUE time series.
val timeseriesName = "VAL_23-ESDV-92501A-PST:VALUE"
val timeseries = spark.sql(s"select * from datapoints where name = '$timeseriesName'")

// Read aggregate data from the same time series
val timeseriesAggregated = spark.sql(s"select * from datapoints where name = '$timeseriesName'" +
s"and aggregation = 'min' and granularity = '1d'")
```

### Events

https://doc.cognitedata.com/concepts/#events

```scala
// Read events from `publicdata`
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "publicdataApiKey")
  .option("type", "events")
  .load()

// Insert the events in your own project using .save()
import org.apache.spark.sql.functions._
df.withColumn("source", lit("publicdata"))
  .write.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("onconflict", "abort")
  .save()

// Get a reference to the events in your project
val myProjectDf = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("type", "events")
  .load()
myProjectDf.createTempView("events")

// Update the description of all events from Open Industrial Data
spark.sql("""
 |select 'Manually copied data from publicdata' as description,
 |source,
 |sourceId
 |from events
 |where source = 'publicdata'
""".stripMargin)
.write.format("com.cognite.spark.datasource")
.option("apiKey", "myApiKey")
.option("onconflict", "update")
.save()
```

### Files metadata

https://doc.cognitedata.com/api/0.6/#operation/getFiles

```scala
// Read files metadata from publicdata
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("type", "files")
  .load()

df.groupBy("fileType").count().show()
```

### 3D models and revisions

https://doc.cognitedata.com/api/0.6/#tag/3D

Note that Open Industrial Data does not have 3D models in it, so to test this you'll need a project
with existing 3D models. There are five options for listing metadata about 3D models:
`3dmodels`, `3dmodelrevisions`, `3dmodelrevisionmappings`, `3dmodelrevisionnodes` and `3dmodelrevisionsectors`.

```scala
// Read 3D models metadata from a project with 3D models and revisions
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "apiKeyToProjectWith3dModels")
  .option("type", "3dmodels")
  .load()

df.show()
```

### Raw tables

https://doc.cognitedata.com/api/0.5/#tag/Raw

Raw tables are organized in databases and tables so you'll need to provide these as options to the DataFrameReader.
`publicdata` does not contain any raw tables so you'll need access to a project with raw table data.

Two additonal options are required:
- `database`: The name of the database in Cognite Data Fusion's "raw" storage to use.
It must exist, and will not be created if it does not.
- `table`: The name of the table in Cognite Data Fusion's "raw" storage to use.
It must exist in the given `database` option, and will not be created if it does not.

You can optionally have Spark infer the DataFrame schema with the following options:
- `inferSchema`: Set this to `"true"` to enable schema inference.
The inferred schema can also be used for inserting new rows.
- `inferSchemaLimit`: The number of rows to use for inferring the schema of this table.
The default is to read all rows.

```scala
val df = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("type", "raw")
  .option("database", "database-name") // a raw database from your project
  .option("table", "table-name") // name of a table in "database-name"
  .load()
df.createTempView("tablename")

// Insert some new values
spark.sql("""insert into tablename values ("key", "values")""")
```
