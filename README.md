# Spark Data Source

The Cognite Spark Data Source lets you use [Spark](https://spark.apache.org/) to read and write data from and to [Cognite Data Fusion](https://docs.cognite.com/dev/) (CDF).

Reads and writes are done in parallel using asynchronous calls.

The instructions below explain how to read from, and write to, the different resource types in CDF.

**In this article**

- [Read and write to CDF](#read-and-write-to-cdf)
    - [Common options](#common-options)
    - [Read data](#read-data)
    - [Write data](#write-data)
    - [Delete data](#delete-data)
- [Examples by resource types](#examples-by-resource-types)
    - [Assets](#assets)
    - [Asset types](#asset-types)
    - [Time series](#time-series)
    - [Data points](#data-points)
    - [Events](#events)
    - [Files metadata](#files-metadata)
    - [3D models and revisions metadata](#3d-models-and-revisions-metadata)
    - [Raw tables](#raw-tables)
- [Build the project with sbt](#build-the-project-with-sbt)
    - [Set up](#set-up)
    - [Run the tests](#run-the-tests)
- [Run the project locally with spark-shell](#run-the-project-locally-with-spark-shell)

## Read and write to CDF

The Cognite Spark Data Source lets you read data from and write data to these resource types: **assets**, **time series**, **data points**, **events**, and **raw tables**. For **files** and **3D models**, you can read **metadata** .

### Common options

Some options are common to all resource types. To set the options, use `spark.read.format("cognite.spark.v1").option("nameOfOption", "value")`.

The common options are:

|    Option     |                                                                                                                                                    Description                                                                                                                                                    |                  Required                  |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `apiKey`      | The CDF [API key](https://doc.cognitedata.com/dev/guides/iam/authentication.html#api-keys) for authorization.                                                                                                                                                                                                     | Yes, if you don't specify a `bearerToken`. |
| `bearerToken` | The CDP [token](https://doc.cognitedata.com/dev/guides/iam/authentication.html#tokens) for authorization.                                                                                                                                                                                                         | Yes, if you don't specify an `apiKey`.      |
| `type`        | The Cognite Data Fusion resource type. See below for more [resource type examples](#examples-by-resource-types).                                                                                                                                                                                                                        | Yes                                        |
| `maxRetries`  | The maximum number of retries to be made when a request fails. Default: 10                                                                                                                                                                                                                                        |                                            |
| `limitPerPartition`       | The number of items to fetch for this resource type to create the DataFrame. Note that this is different from the SQL `SELECT * FROM ... LIMIT 1000` limit. This option specifies the limit for items to fetch from CDF *per partition*, *before* filtering and other transformations are applied to limit the number of results. Not supported by data points. |                                            |
| `batchSize`   | The maximum number of items to read/write per API call.                                                                                                                                                                                                                                                           |                                            |

### Read data

To read from CDF resource types, you need to specify: an **API-key** or a **bearertoken** and the **resource type** you want to read from. To read from a table you also need to specify the database and table names.

**Filter pushdown**

For some fields, filters are pushed down to the API. For example, if you read events with a filter on IDs,  only the IDs that satisfy the filter are read from CDF, as opposed to reading all events and **then** applying  the filter. This happens automatically, but note that filters are only pushed down when Spark reads data from CDF, and not when working on a DataFrame that is already in memory.

The following fields have filter pushdown:


|Resource type  |Fields  |
|---------|---------|
|Assets     | - name</br>- source</br>        |
|Events     | - source</br>- assetIds</br>- type</br>- subtype (You must supply a filter on type when filtering on subtype)</br>- minStartTime</br>- maxStartTime        |
|Time Series     | - assetId</br>        |

### Write data

You can write to CDF with:

- `insertInto` - checks that all fields are present and in the correct order. Can be more convenient when you're working with Spark SQL tables.

- `save` - gives you control over how to handle potential collisions with existing data, and allows you to update a subset of fields in a row.

**`.insertInto()`**

To write to a resource using the insert into pattern, you'll need to register a DataFrame that was read from
the resource, as a temporary view. You also need write access to the project and resources. In the examples below, replace  `myApiKey` with your own API key.

Your schema must match that of the target exactly. To ensure this, copy the schema from the DataFrame you read into with `sourceDf.select(destinationDf.columns.map(col):_*)`. See the [time series example below](#time-series). 

`.insertInto()` does upserts (updates existing rows and inserts new rows) for events and time series. Events are matched on
`source+sourceId` while time series are matched on `id`. 

For assets, raw tables and data points, `.insertInto()` does inserts and throws an error if one or more rows already exist.

**`.save()`**

We currently support writing with `.save()` for assets, events, and time series. You'll need to provide an API key and the resource type you want to write to. You can also use `.option("onconflict", value)` to specify the desired behavior when rows in your Dataframe are present in CDF.

The valid options for onconflict are:

- `abort` - tries to insert all rows in the Dataframe. Throws an error if the resource item already exists, and no more rows will be written.

- `update` - looks for all rows in the Dataframe in CDF and tries to update them. If one or more rows do not exist, no more rows are updated and an error is thrown. Supports partial updates.

- `upsert` - updates rows that already exist, and inserts new rows.

See an example of using `.save()` under [Events below](#events).

### Delete data

We currently support deleting with `.save()` for assets, events and time series.

You need to provide an API key and specify the resource type, and then specify `delete` as the `onconflict` option like this: `.option("onconflict", "delete")`.

See an example for using `.save()` to delete under [Time Series below](#time-series).

## Examples by resource types

### Assets

Learn more about assets [here](https://doc.cognitedata.com/dev/concepts/resource_types/assets.html).

```scala
// Read assets from your project into a DataFrame
val df = spark.sqlContext.read.format("cognite.spark.v1")
 .option("apiKey", "myApiKey")
 .option("type", "assets")
 .load()

// Register your assets in a temporary view
df.createTempView("assets")

// Create a new asset and write to CDF
// Note that parentId, asset type IDs, and asset type field IDs have to exist.
val assetColumns = Seq("externalId", "name", "parentId", "description", "metadata", "source",
"id", "createdTime", "lastupdatedTime")
val someAsset = Seq(
("Some external ID", "asset name", "This is another asset", Map("sourceSystem"->"MySparkJob"), "some source", 
99L, 0L, 0L))
val someAssetDf = someAsset.toDF(assetColumns:_*)

// Write the new asset to CDF, ensuring correct schema by borrowing the schema of the df from CDF
spark
  .sqlContext
  .createDataFrame(someAssetDf.rdd, df.schema)
  .write
  .insertInto("assets")
```

### Time series

Learn more about time series [here](https://doc.cognitedata.com/dev/concepts/resource_types/timeseries.html).

```scala
// Get all the time series from your project
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "timeseries")
  .load()
df.createTempView("timeseries")

// Read some new time series data from a csv file
val timeSeriesDf = spark.read.format("csv")
  .option("header", "true")
  .load("timeseries.csv)

// Ensure correct schema by copying the columns in the DataFrame read from the project.
// Note that the time series must already exist in the project before data can be written to it, based on the ´name´ column.
timeSeriesDf.select(df.columns.map(col):_*)
  .write
  .insertInto("timeseries")

// Delete all time series you just created
timeSeriesDf
  .write
  .format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "timeseries")
  .option("onconflict", "delete")
  .save()
```

### Data points

Data points are always related to a time series. To read data points you need to filter by a valid time series id, otherwise an empty DataFrame is returned. **Important**: Be careful when using caching with this resource type. If you cache the result of a filter and then apply another filter, you do not trigger more data to be read from CDF and end up with an empty DataFrame.

#### Numerical data points

To read numerical data points from CDF, use the `.option("type", "datapoints")` option. For numerical data points you can also request aggregated data by filtering by **aggregation** and **granularity**.

- `aggregation`: Numerical data points can be aggregated to reduce the amount of data transferred in query responses and improve performance. You can specify one or more aggregates (for example average, minimum and maximum) and also the time granularity for the aggregates (for example 1h for one hour). If the aggregate option is NULL, or not set, data points return the raw time series data.

- `granularity`: Aggregates are aligned to the start time modulo of the granularity unit. For example, if you ask for daily average temperatures since Monday afternoon last week, the first aggregated data point contains averages for the whole of Monday, the second for Tuesday, etc.

```scala
// Get the datapoints from publicdata
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "publicdataApiKey")
  .option("type", "datapoints")
  .load()

// Create the view to enable SQL syntax
df.createTempView("datapoints")

// Read the raw datapoints from the VAL_23-FT-92537-04:X.Value time series.
val timeseriesId = 3385857257491234L
val timeseries = spark.sql(s"select * from datapoints where id = $timeseriesId")

// Read aggregate data from the same time series
val timeseriesAggregated = spark.sql(s"select * from datapoints where id = $timeseriesId" +
s"and aggregation = 'min' and granularity = '1d'")
```

#### String data points

To read string data points from CDF, provide the `.option("type", "stringdatapoints")` option.

```scala
// Get the datapoints from publicdata
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "publicdataApiKey")
  .option("type", "stringdatapoints")
  .load()

// Create the view to enable SQL syntax
df.createTempView("stringdatapoints")

// Read the raw datapoints from the VAL_23-PIC-96153:MODE time series.
val timeseriesId = 6536948395539605L
val timeseries = spark.sql(s"select * from stringdatapoints where id = $timeseriesId")
```

### Events

Learn more about events [here](https://doc.cognitedata.com/dev/concepts/resource_types/events.html)

```scala
// Read events from `publicdata`
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "publicdataApiKey")
  .option("type", "events")
  .load()

// Insert the events in your own project using .save()
import org.apache.spark.sql.functions._
df.withColumn("source", lit("publicdata"))
  .write.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("onconflict", "abort")
  .save()

// Get a reference to the events in your project
val myProjectDf = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "events")
  .load()
myProjectDf.createTempView("events")

// Update the description of all events from Open Industrial Data
spark.sql("""
 |select 'Manually copied data from publicdata' as description,
 |id,
 |from events
 |where source = 'publicdata'
""".stripMargin)
.write.format("cognite.spark.v1")
.option("apiKey", "myApiKey")
.option("onconflict", "update")
.save()
```

### Files metadata

Learn more about files [here](https://doc.cognitedata.com/dev/concepts/resource_types/files.html)

```scala
// Read files metadata from publicdata
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "files")
  .load()

df.groupBy("fileType").count().show()
```

### 3D models and revisions metadata

Learn more about 3D models and revisions [here](https://doc.cognitedata.com/dev/concepts/resource_types/3dmodels.html)

Note that the Open Industrial Data project does not have any 3D models. To test this example, you need a project with existing 3D models. There are four options for listing metadata about 3D models:
`3dmodels`, `3dmodelrevisions`, `3dmodelrevisionmappings` and `3dmodelrevisionnodes`.

```scala
// Read 3D models metadata from a project with 3D models and revisions
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "apiKeyToProjectWith3dModels")
  .option("type", "3dmodels")
  .load()

df.show()
```

### Raw tables

Learn more about Raw tables [here](https://doc.cognitedata.com/api/v1/#tag/Raw).

Raw tables are organized in databases and tables that you need to provide as options to the DataFrameReader. `publicdata` does not contain any raw tables so you'll need access to a project with raw table data.

Two additional options are required:

- `database`: The name of the database in Cognite Data Fusion's "raw" storage to use. The database must exist, and will not be created if it does not.

- `table`: The name of the table in Cognite Data Fusion's "raw" storage to use. The table must exist in the database specified in the `database` option, and will not be created if it does not.

Optionally, you can have Spark infer the DataFrame schema with the following options:

- `inferSchema`: Set this to `"true"` to enable schema inference. You can also use the inferred schema can also be used for inserting new rows.

- `inferSchemaLimit`: The number of rows to use for inferring the schema of the table. The default is to read all rows.

```scala
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "raw")
  .option("database", "database-name") // a raw database from your project
  .option("table", "table-name") // name of a table in "database-name"
  .load()
df.createTempView("tablename")

// Insert some new values
spark.sql("""insert into tablename values ("key", "values")""")
```

## Build the project with sbt

The project runs read-only integration tests against the Open Industrial Data project. Navigate to
https://openindustrialdata.com/ to get an API key and store it in the `TEST_API_KEY_READ` environment variable.

To run the write integration tests, you'll also need to set the `TEST_API_KEY_WRITE` environment variable to an API key for a project where you have write access.

<!-- To run tests against greenfield, set the `TEST_API_KEY_GREENFIELD` environment variable to an API key with read access to the project cdp-spark-datasource-test. [_Arne: Is this internal? If not, how do I get an API key?_] -->

### Set up

1. First run `sbt compile` to generate Scala sources for protobuf.

2. If you have set `TEST_API_KEY_WRITE`, run the Python files `scripts/createThreeDData.py` and `scripts/createFilesMetaData.py` (You need to install the cognite-sdk-python and set the `PROJECT` and `TEST_API_KEY_WRITE` environment variables).

This uploads a 3D model to your project that you can use for testing.

### Run the tests

To run **all tests**, run `sbt test`.

To run **groups of tests**, enter sbt shell mode `sbt>`

To run **only the read-only tests**, run `sbt> testOnly -- -n ReadTest`

To run **only the write tests**, run `sbt> testOnly -- -n WriteTest`

<!-- To run **only the greenfield tests**, run `sbt> testOnly -- -n GreenfieldTest`-->

To run **all tests except the write tests**, run `sbt> testOnly -- -l WriteTest`

To **skip the read/write tests in assembly**, add `test in assembly := {}` to build.sbt, or run:

- Windows: `sbt "set test in assembly := {}" assembly`

- Linux/macos: `sbt 'set test in assembly := {}' assembly`

## Run the project locally with spark-shell

To download the spark data source, simply add the maven coordinates for the package using the
`--packages` flag.

Get an API-key for the Open Industrial Data project at https://openindustrialdata.com and run the following commands (replace <release> with the release you'd like, for example 1.2.0):

``` cmd
$> spark-shell --packages com.cognite.spark.datasource:cdf-spark-datasource_2.11:<latest-release>
scala> val apiKey="secret-key-you-have"
scala> val df = spark.sqlContext.read.format("cognite.spark.v1")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limitPerPartition", "1000")
  .option("type", "assets")
  .load()

df: org.apache.spark.sql.DataFrame = [name: string, parentId: bigint ... 3 more fields]

scala> df.count
res0: Long = 1000
```

Note that if you're on an older version than `1.1.0` you'll need to use the old name,
`cdp-spark-datasource`.