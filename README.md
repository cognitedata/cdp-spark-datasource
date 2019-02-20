# Spark data source for CDP API

Supports read and write for raw and clean data types.

Reads and writes are done in parallel using asynchronous calls.
Reads will start only after all cursors have been retrieved, which can take a while
if there are many items.

See instructions below for examples using different resource types.

## Changelog

### 0.4.1

- Read-only support for files metadata.
- Initial read-only support for 3D data (should be considered an *alpha feature, may not work*).

### 0.4.0

- *Breaking change* `"tables"` renamed to `"raw"`.
- Improved performance for assets.
- Validation of `key` column for raw tables, null values are not allowed.
- Retries on error code 500 responses.
- New `maxRetries` option for all resource types to set the number of retries.
- Improved back off algorithm for retries.
- `project` is no longer a necessary option, it will be retrieved from the API key.

## Build the project with sbt:

The project runs read-only integration tests against the Open Industrial Data project. Head over to
https://openindustrialdata.com/ to get an API key and store it in the environment variable `TEST_API_KEY_READ`.
To run the write integration tests you'll also need to set the environment variable `TEST_API_KEY_WRITE`
to an API key to a project where you have write access.

First run `sbt compile` to generate Scala sources for protobuf.

To run all tests run `sbt test`.

To run groups of tests enter sbt shell mode `sbt>`

To run only the read-only tests run `sbt> testOnly -- -n ReadTest`

To run only the write tests run `sbt> testOnly -- -n WriteTest`

To run all tests except the write tests run `sbt> testOnly -- -l WriteTest`

Run `sbt assembly` to create `~/path-to-repo/target/scala-2.11/cdp-spark-datasource-*-jar-with-dependencies.jar`.

To skip the read/write tests in assembly you can add `test in assembly := {}` to build.sbt, or run:

Windows: `sbt "set test in assembly := {}" assembly`

Linux/macos: `sbt 'set test in assembly := {}' assembly`


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

## Reading and writing Cognite Data Platform resource types:

cdp-spark-datasource supports reads from, and writes to, assets, time series, raw tables, data points and events.

### Reading

To read from CDP resource types you need to provide two things:
an API-key and the resource type. To read from a table you should also specify the database name and table name.

### Writing

To write to a resource you'll need to register a DataFrame that was read from
that resource as a temporary view. You'll need a project where you have write access
and replace `myApiKey` in the examples below.

Your schema will have to match that of the target exactly. A convenient way to ensure
this is to copy the schema from the DataFrame you read into with `sourceDf.select(destinationDf.columns.map(col):_*)`, see time series example.

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
val someAsset = Seq(("99-BB-99999",99L,"This is another asset",Map("sourceSystem"->"MySparkJob"),99L))
val someAssetDf = someAsset.toDF("name", "parentID", "description","metadata","id")
someAssetDf
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
// Note that the timeseries must already exist in the project before data can be written to it, based on the ´name´ column.
timeSeriesDf.select(destinationDf.columns.map(col):_*)
  .write
  .insertInto("timeseries")
```

### Data points

https://doc.cognitedata.com/concepts/#data-points

Data points are always related to a time series. To read datapoints you will need to filter by a valid time series name.
You can also request aggregated data by filtering by aggregation and granularity.

`aggregation`: Numerical data points can be aggregated before they are retrieved from CDP.
This allows for faster queries by reducing the amount of data transferred.
You can aggregate data points by specifying one or more aggregates (e.g. average, minimum, maximum)
as well as the time granularity over which the aggregates should be applied (e.g. “1h” for one hour).
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

// Get a reference to the events in your project
val myProjectDf = spark.read.format("com.cognite.spark.datasource")
  .option("apiKey", "myApiKey")
  .option("type", "events")
  .load()
myProjectDf.createTempView("events")

// Copy the Valhall events to your project
df.filter($"subtype" === "Valhall")
  .write
  .insertInto("events")
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

### Raw tables

https://doc.cognitedata.com/api/0.5/#tag/Raw

Raw tables are organized in databases and tables so you'll need to provide these as options to the DataFrameReader.
`publicdata` does not contain any raw tables so you'll need access to a project with raw table data.
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

## Short-term list of missing features:

- Multi-tags (finally figured out how to best do it)
- implement logging according to the standard spark way
- streaming support
- figure out how to expose metadata
- figure out how to expose aggregates in a good way
