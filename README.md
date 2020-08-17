![Maven Central](https://img.shields.io/maven-central/v/com.cognite.spark.datasource/cdf-spark-datasource_2.12?label=version)
# Spark Data Source

The Cognite Spark Data Source lets you use [Spark](https://spark.apache.org/) to read and write data from and to [Cognite Data Fusion](https://docs.cognite.com/dev/) (CDF).

Reads and writes are done in parallel using asynchronous calls.

The instructions below explain how to read from, and write to, the different resource types in CDF.

**In this article**

- [Spark Data Source](#spark-data-source)
  - [Read and write to CDF](#read-and-write-to-cdf)
    - [Common options](#common-options)
    - [Read data](#read-data)
    - [Write data](#write-data)
    - [Delete data](#delete-data)
  - [Asset hierarchy builder (beta)](#asset-hierarchy-builder-beta)
    - [Requirements](#requirements)
    - [Options](#options)
    - [Setup for Python](#setup-for-python)
    - [Example (Scala)](#example-scala)
    - [Example (Python)](#example-python)
  - [Schemas](#schemas)
    - [Assets schema](#assets-schema)
    - [Events schema](#events-schema)
    - [Files schema](#files-schema)
    - [Data points schema](#data-points-schema)
    - [String data points schema](#string-data-points-schema)
    - [Time series schema](#time-series-schema)
    - [Asset Hierarchy](#asset-hierarchy)
  - [Examples by resource types](#examples-by-resource-types)
    - [Assets](#assets)
    - [Time series](#time-series)
    - [Data points](#data-points)
      - [Numerical data points](#numerical-data-points)
      - [String data points](#string-data-points)
    - [Events](#events)
    - [Files metadata](#files-metadata)
    - [3D models and revisions metadata](#3d-models-and-revisions-metadata)
    - [RAW tables](#raw-tables)
  - [Build the project with sbt](#build-the-project-with-sbt)
    - [Set up](#set-up)
    - [Run the tests](#run-the-tests)
  - [Run the project locally with spark-shell](#run-the-project-locally-with-spark-shell)

## Read and write to CDF

The Cognite Spark Data Source lets you read data from and write data to these resource types: **assets**, **time series**, **data points**, **events**, and **RAW tables**. For **files** and **3D models**, you can read **metadata** .

### Common options

Some options are common to all resource types. To set the options, use `spark.read.format("cognite.spark.v1").option("nameOfOption", "value")`.

The common options are:

|    Option     |                                                                                                                                                    Description                                                                                                                                                    |                  Required                  |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `apiKey`      | The CDF [API key](https://doc.cognitedata.com/dev/guides/iam/authentication.html#api-keys) for authorization.                                                                                                                                                                                                     | Yes, if you don't specify a `bearerToken`. |
| `bearerToken` | The CDF [token](https://doc.cognitedata.com/dev/guides/iam/authentication.html#tokens) for authorization.                                                                                                                                                                                                         | Yes, if you don't specify an `apiKey`.      |
| `project`     |  The CDF project. By default it's inferred from the API key. |  |
| `type`        | The Cognite Data Fusion resource type. See below for more [resource type examples](#examples-by-resource-types).                                                                                                                                                                                                                        | Yes                                        |
| `maxRetries`  | The maximum number of retries to be made when a request fails. Default: 10                                                                                                                                                                                                                                        |                                            |
| `limitPerPartition`       | The number of items to fetch for this resource type to create the DataFrame. Note that this is different from the SQL `SELECT * FROM ... LIMIT 1000` limit. This option specifies the limit for items to fetch from CDF *per partition*, *before* filtering and other transformations are applied to limit the number of results. Not supported by data points. |                                            |
| `batchSize`   | The maximum number of items to read/write per API call.                                                                                                                                                                                                                                                           |                                            |
| `baseUrl`     | Address of the CDF API. For example might be changed to https://greenfield.cognitedata.com. By default it is set to https://api.cognitedata.com                        |   |
| `collectMetrics` | `true` or `false` - if Spark metrics should be collected about number of reads, inserts, updates and deletes |
| `metricsPrefix` | Common prefix for all collected metrics. Might be useful when working with multiple connections. |
| `partitions`   | Number of [CDF partitions](https://docs.cognite.com/dev/concepts/pagination/#parallel-retrieval) to use. By default it's 200. |
| `parallelismPerPartition` | How many parallel request should run for one Spark partition. Number of Spark partitions = `partitions` / `parallelismPerPartition` |
| `applicationName` | Identifies the application making requests by including a `X-CDP-App` header. Defaults to `com.cognite.spark.datasource-(version)` |
| `clientTag` | If set, will be included as a `X-CDP-ClientTag` header in requests. This is typically used to group sets of requests as belonging to some definition of a job or workload for debugging. |

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

`.insertInto()` does upserts (updates existing rows and inserts new rows) for events, assets, and time series.
`.insertInto()` upserts use `externalId` (ignoring `id`), attempting to create a row with `externalId`
if set, and if a row with the given `externalId` already exists it will be updated.

Data points also have upsert behavior, but based on the timestamp.

For RAW tables, `.insertInto()` does inserts and throws an error if one or more rows already exist.
For files, `insertInto()` only supports updating existing files.

**`.save()`**

We currently support writing with `.save()` for assets, events, and time series. You'll need to provide an API key and the resource type you want to write to. You can also use `.option("onconflict", value)` to specify the desired behavior when rows in your Dataframe are present in CDF.

The valid options for onconflict are:

- `abort` - tries to insert all rows in the Dataframe. Throws an error if the resource item already exists, and no more rows will be written.

- `update` - looks for all rows in the Dataframe in CDF and tries to update them. If one or more rows do not exist, no more rows are updated and an error is thrown. Supports partial updates.

- `upsert` - updates rows that already exist, and inserts new rows.
In this mode inserted rows with `id` set will always attempt to update the target row with such an `id`.
If `id` is null, or not present, and `externalId` is not null, it will attempt to create a row with the
given `externalId`. If such a row already exists, that row will be updated to the values present in the
row being inserted.

Multiple rows with the same `id` and `externalId` are allowed for
upserts, but the order in which they are applied is undefined and we currently
only guarantee that at least one upsert will be made for each `externalId`,
and at least one update will be made for each `id` set.

This is based on the assumption that upserts for the same `id` or `externalId`
will have the same values. If you have a use case where this is not the case,
please let us know.

See an example of using `.save()` under [Events below](#events).

### Delete data

We currently support deleting with `.save()` for assets, events and time series.

You need to provide an API key and specify the resource type, and then specify `delete` as the `onconflict` option like this: `.option("onconflict", "delete")`.

See an example for using `.save()` to delete under [Time Series below](#time-series).

Assets and events will ignore existing ids on deletes. If you prefer to abort the job
when attempting to delete an unknown id, use `.option("ignoreUnknownIds", "false")`
for those resources types.

Expected schema for delete of Time series, Assets, Events or Files is:

| Column name       | Type                  |  Nullable |
| ------------------| ----------------------| --------- |
| `id`              | `long`                | No        |

Expected schema for delete of Datapoints or String Datapoints is:

| Column name       | Type                  |  Nullable |
| ------------------| ----------------------| --------- |
| `id`             |  `long`                Yes      |
| `externalId`      | `string`             | Yes     |
| `inclusiveBegin`  | `timestamp`          | Yes     |
| `exclusiveBegin`  | `timestamp`          | Yes     |
| `inclusiveEnd`    | `timestamp`          | Yes     |
| `exclusiveEnd`    | `timestamp`          | Yes     |

One of `id` & `externalId`, `inclusiveBegin` & `exclusiveBegin` and `inclusiveEnd` & `exclusiveEnd` must be specified.
Data points are deleted by a range and both bound must be specified.
To delete a single data point, set `inclusiveBegin` and `inclusiveEnd` to the same value.
To delete a range between two points, set `exclusiveBegin` to the first point and `exclusiveEnd` to the second one;
this will not delete the boundaries, but everything between them.

## Asset hierarchy builder (beta)
Note: The asset hierarchy builder is currently in beta, and has not been sufficiently tested to be used 
on production data.

The `.option("type", "assethierarchy")` lets you write new asset hierarchies, or update existing ones,
using the Spark Data Source.
The asset hierarchy builder can ingest entire hierarchies of nodes connected
through the `externalId`/`parentExternalId` relationship. If input contains an update to data that already exists,
i.e there's a match on `externalId` and there's a change to one of the other fields, the asset will be updated.
There's also an option to delete assets from CDF that are not referenced in the input data.

### Requirements
- Root assets are denoted by setting their `parentExternalId` to the empty string `""`.
- The input data must not have loops, to ensure all asset hierarchies are fully connected.
- `externalId` can not be the empty string `""`.

### Options
|    Option                  | Default | Description |
| -------------------------- | --------|------------------------------------------------------------------------------------------------------------------ |
| `deleteMissingAssets`      | `false` | Whether or not you would like assets under the root to be deleted if they're not present in the input data.       |
| `subtrees` | `ingest` | Controls what should happen with subtrees without a root node in the input data. `ingest` says they will be processed and loaded into CDF, `ignore` will ignore all of them and `error` will stop the execution and raise an error (nothing will be ingested). |
| `batchSize`                | 1000    | The number of assets to write per API call.                                                                       |

### Setup for Python

You may want to set up a Jupyter notebook with `pySpark` running.

* Download spark version `2.4.5` [here](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz)

* Follow the instructions given [here](https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes), except that your Spark version will be `2.4.5`.

* Start your Jupyter notebook with the following command (instead of `pyspark` as in the link above):

```
pyspark --packages com.cognite.spark.datasource:cdf-spark-datasource_2.11:1.2.18
```

### Example (Scala)

```scala
val assetHierarchySchema = Seq("externalId", "parentExternalId", "source", "description", "name", "metadata")

// Manually create some assets that satisfy the requirements of the asset hierarchy builder
val assetHierarchy = Seq(
  ("root_asset", "", "manual_input", "root_asset", Some("This is the root asset"), Map("asset_depth" -> "0")),
  ("first_child", "root_asset", "manual_input", "first_child", Some("This is the first_child"), Map("asset_depth" -> "1")),
  ("second_child", "root_asset", "manual_input", "second_child", Some("This is the second_child"), Map("asset_depth" -> "1")),
  ("grandchild", "first_child", "manual_input", "grandchild", Some("This is the child of first_child"), Map("asset_depth" -> "2"))
)

val assetHierarchyDataFrame = spark
  .sparkContext
  .parallelize(assetHierarchy)
  .toDF(assetHierarchySchema:_*)

// Validate that the schema is as expected
assetHierarchyDataFrame.printSchema()

// Insert the assets with the asset hierarchy builder
assetHierarchyDataFrame.write
  .format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "assethierarchy")
  .save()

// Have a look at your new asset hierarchy
spark.read
  .format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "assets")
  .load()
  .where("source = 'manual_input'")
  .show()

// Delete everything but the root using the deleteMissingAssets flag
spark
  .sparkContext
  .parallelize(Seq(Seq("root_asset", "", "manual_input", "root_asset", Some("This is the root asset"), None)))
  .toDF(assetHierarchySchema)
  .format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "assethierarchy")
  .option("deleteMissingAssets", "true")
  .save()
```

### Example (Python)

```python
assetHierarchySchema = ["externalId", "parentExternalId", "source", "description", "name", "metadata"]

# Manually create some assets that satisfy the requirements of the asset hierarchy builder
assetHierarchy = [
  ["root_asset", "", "manual_input", "root_asset", "This is the root asset", {"asset_depth" : "0"}],
  ["first_child", "root_asset", "manual_input", "first_child", "This is the first_child", {"asset_depth" : "1"}],
  ["second_child", "root_asset", "manual_input", "second_child", "This is the second_child", {"asset_depth" : "1"}],
  ["grandchild", "first_child", "manual_input", "grandchild", "This is the child of first_child", {"asset_depth" : "2"}]
]

assetHierarchyDataFrame = spark.sparkContext.parallelize(assetHierarchy).toDF(assetHierarchySchema)

# Validate that the schema is as expected
assetHierarchyDataFrame.printSchema()

# Insert the assets with the asset hierarchy builder
assetHierarchyDataFrame.write \
    .format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "assethierarchy") \
    .save()

# Have a look at your new asset hierarchy
spark.read \
  .format("cognite.spark.v1") \
  .option("apiKey", myApiKey) \
  .option("type", "assets") \
  .load() \
  .where("source = 'manual_input'") \
  .show()

# Delete everything but the root using the deleteMissingAssets flag
spark \
    .sparkContext \
    .parallelize([["root_asset", "", "manual_input", "root_asset", "This is the root asset", {"":""}]]) \
    .toDF(assetHierarchySchema) \
    .write \
    .format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "assethierarchy") \
    .option("deleteMissingAssets", "true") \
    .save()
```

## Schemas

Spark DataFrames have schemas, with typing and names for columns. When writing to a resource in CDF using
the `insertInto`-pattern you have to match the schema exactly (see [.insertInto()](#`.insertInto()`) for a tip about this).

The schemas mirror the CDF API as closely as possible.

### Assets schema
| Column name       | Type                  |  Nullable |
| ------------------| ----------------------| --------- |
| `externalId`      | `string`              | Yes       |
| `name`            | `string`              | No        |
| `parentId`        | `long`                | Yes       |
| `description`     | `string`              | Yes       |
| `metadata`        | `map(string, string)` | Yes       |
| `source`          | `long`                | Yes       |
| `id`              | `long`                | No        |
| `createdTime`     | `timestamp`           | No        |
| `lastUpdatedTime` | `timestamp`           | No        |
| `rootId`          | `long`                | Yes       |
| `aggregates`      | `map(string, long)`   | Yes       |
| `dataSetId`       | `long`                | Yes       |

### Events schema
| Column name       | Type                  |  Nullable |
| ------------------| ----------------------| --------- |
| `id`              | `long`                | No        |
| `startTime`       | `timestamp`           | Yes       |
| `endTime`         | `timestamp`           | Yes       |
| `description`     | `string`              | Yes       |
| `type`            | `string`              | Yes       |
| `subtype`         | `string`              | Yes       |
| `metadata`        | `map(string, string)` | Yes       |
| `assetIds`        | `array(long)`         | Yes       |
| `source`          | `long`                | Yes       |
| `externalId`      | `string`              | Yes       |
| `createdTime`     | `timestamp`           | No        |
| `lastUpdatedTime` | `timestamp`           | No        |
| `dataSetId`       | `long`                | Yes       |

### Files schema
| Column name          | Type                  |  Nullable |
| ---------------------| ----------------------| --------- |
| `id`                 | `long`                | No        |
| `name`               | `string`              | No        |
| `source`             | `long`                | Yes       |
| `externalId`         | `string`              | Yes       |
| `mimeType`           | `string`              | Yes       |
| `metadata`           | `map(string, string)` | Yes       |
| `assetIds`           | `array(long)`         | Yes       |
| `uploaded`           | `boolean`             | No        |
| `uploadedTime`       | `timestamp`           | Yes       |
| `createdTime`        | `timestamp`           | No        |
| `lastUpdatedTime`    | `timestamp`           | No        |
| `sourceCreatedTime`  | `timestamp`           | Yes       |
| `sourceModifiedTime` | `timestamp`           | Yes       |
| `securityCategories` | `array(long)`         | Yes       |
| `uploadUrl`          | `string`              | Yes       |
| `dataSetId`          | `long`                | Yes       |

### Data points schema
| Column name   | Type        |  Nullable |
| --------------| ------------| --------- |
| `id`          | `long`      | Yes       |
| `externalId`  | `string`    | Yes       |
| `timestamp`   | `timestamp` | No        |
| `value`       | `double`    | No        |
| `aggregation` | `string`    | Yes       |
| `granularity` | `string`    | Yes       |

### String data points schema
| Column name   | Type        |  Nullable |
| --------------| ------------| --------- |
| `id`          | `long`      | Yes       |
| `externalId`  | `string`    | Yes       |
| `timestamp`   | `timestamp` | No        |
| `value`       | `string`    | No        |

### Time series schema
| Column name          | Type                  |  Nullable |
| ---------------------| ----------------------| --------- |
| `name`               | `string`              | Yes       |
| `isString`           | `boolean`             | No        |
| `metadata`           | `map(string, string)` | Yes       |
| `unit`               | `string`              | Yes       |
| `assetId`            | `long`                | Yes       |
| `isStep`             | `boolean`             | No        |
| `description`        | `string`              | Yes       |
| `securityCategories` | `array(long)`         | Yes       |
| `id`                 | `long`                | No        |
| `externalId`         | `string`              | Yes       |
| `createdTime`        | `timestamp`           | No        |
| `lastUpdatedTime`    | `timestamp`           | No        |
| `dataSetId`          | `long`                | Yes       |


### Asset Hierarchy
| Column name          | Type                  |  Nullable |
| ---------------------| ----------------------| --------- |
| `externalId`         | `string`              | No        |
| `parentExternalId`   | `string`              | No        |
| `source`             | `string`              | Yes       |
| `name`               | `string`              | No        |
| `description`        | `string`              | Yes       |
| `metadata`           | `map(string, string)` | Yes       |
| `dataSetId`          | `long`                | Yes       |

### Sequences
| Column name          | Type                  |  Nullable |
| ---------------------| ----------------------| --------- |
| `externalId`         | `string`              | Yes        |
| `name`   | `string`              | Yes        |
| `description`             | `string`              | Yes       |
| `assetId`               | `long`              | Yes        |
| `metadata`           | `map(string, string)` | Yes       |
| `dataSetId`          | `long`                | Yes       |
| `columns`        | `array(SequenceColumn)`              | No       |

The `columns` field should be an array of `SequenceColumn`s, which are rows with the following fields:

| Column name          | Type                  |  Nullable |
| ---------------------| ----------------------| --------- |
| `externalId`         | `string`              | No        |
| `name`   | `string`              | Yes        |
| `description`             | `string`              | Yes       |
| `valueType`               | `string`              | No        |
| `metadata`           | `map(string, string)` | Yes       |
| `dataSetId`          | `long`                | Yes       |
| `columns`        | `array(SequenceColumn)`              | No       |

### Sequence rows

The schema of `sequencerows` relation matches the sequence that is specified in `id` or `externalId` option. Apart from the sequence columns, there is a non-nullable `rowNumber` column of type `long`

## Examples by resource types

### Assets

Learn more about assets [here](https://doc.cognitedata.com/dev/concepts/resource_types/assets.html).

```scala
// Scala Example. See Python example below.

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

val someAssetDf = spark
  .sparkContext
  .parallelize(someAsset)
  .toDF(assetColumns:_*)

// Write the new asset to CDF, ensuring correct schema by borrowing the schema of the df from CDF
spark
  .sqlContext
  .createDataFrame(someAssetDf.rdd, df.schema)
  .write
  .insertInto("assets")
```

```python
# Python Example

# Read assets from your project into a DataFrame
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "assets") \
    .load()

# Register your assets in a temporary view
df.createTempView("assets")

# Create a new asset and write to CDF
# Note that parentId, asset type IDs, and asset type field IDs have to exist. You might want to change the columns here as per your requirements
assetColumns = ["externalId", "name", "parentId", "description", "metadata", "source", "id", "createdTime", "lastupdatedTime", "rootId", "aggregates", "dataSetId", "parentExternalId"]

someAsset = [["Some external ID", "asset name", 0, "This is another asset", {"sourceSystem": "MySparkJob"}, "some source", 99, 0, 0, "", "", "", ""]]

someAssetDf = spark.sparkContext \
    .parallelize(someAsset) \
    .toDF(assetColumns)


# Write the new asset to CDF, ensuring correct schema by borrowing the schema of the df from CDF
spark.createDataFrame(someAssetDf.rdd, df.schema) \
    .write \
    .insertInto("assets")
```

### Time series

Learn more about time series [here](https://doc.cognitedata.com/dev/concepts/resource_types/timeseries.html).

```scala
// Scala Example. See Python example below.

// Get all the time series from your project
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "timeseries")
  .load()
df.createTempView("timeseries")

// Read some new time series data from a csv file
val timeSeriesDf = spark.read.format("csv")
  .option("header", "true")
  .load("timeseries.csv")

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

```python
# Python Example

# Get all the time series from your project
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "timeseries") \
    .load()

df.createTempView("timeseries")

# Read some new time series data from a csv file
timeSeriesDf = spark.read.format("csv") \
    .option("header", "true") \
    .load("timeseries.csv")

# Ensure correct schema by copying the columns in the DataFrame read from the project.
# Note that the time series must already exist in the project before data can be written to it, based on the ´name´ column.
timeSeriesDf.select(df.columns.map(col)) \
    .write \
    .insertInto("timeseries")

# Delete all time series you just created
timeSeriesDf \
    .write \
    .format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "timeseries") \
    .option("onconflict", "delete") \
    .save()
```

### Data points

Data points are always related to a time series. To read data points you need to filter by a valid time series id, otherwise an empty DataFrame is returned. **Important**: Be careful when using caching with this resource type. If you cache the result of a filter and then apply another filter, you do not trigger more data to be read from CDF and end up with an empty DataFrame.

#### Numerical data points

To read numerical data points from CDF, use the `.option("type", "datapoints")` option. For numerical data points you can also request aggregated data by filtering by **aggregation** and **granularity**.

- `aggregation`: Numerical data points can be aggregated to reduce the amount of data transferred in query responses and improve performance. You can specify one or more aggregates (for example average, minimum and maximum) and also the time granularity for the aggregates (for example 1h for one hour). If the aggregate option is NULL, or not set, data points return the raw time series data.

- `granularity`: Aggregates are aligned to the start time modulo of the granularity unit. For example, if you ask for daily average temperatures since Monday afternoon last week, the first aggregated data point contains averages for the whole of Monday, the second for Tuesday, etc.

```scala
// Scala Example. See Python example below.

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

```python
# Get the datapoints from publicdata
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "datapoints") \
    .load()

# Create the view to enable SQL syntax
df.createTempView("datapoints")

# Read the raw datapoints from the VAL_23-FT-92537-04:X.Value time series.
timeseriesId = 3385857257491234


query = "select * from datapoints where id = %d" % timeseriesId
timeseries = spark.sql(query)

# Read aggregate data from the same time series
timeseriesAggregated = spark.sql("select * from datapoints where id = %d" %timeseriesId  +
" and aggregation = 'min' and granularity = '1d'")
```

#### String data points

To read string data points from CDF, provide the `.option("type", "stringdatapoints")` option.

```scala
// Scala Example. See Python example below.

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

```python
# Python Example

# Get the datapoints from publicdata
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "stringdatapoints") \
    .load()

# Create the view to enable SQL syntax
df.createTempView("stringdatapoints")

# Read the raw datapoints from the VAL_23-PIC-96153:MODE time series.
timeseriesId = 6536948395539605
timeseries = spark.sql("select * from stringdatapoints where id = %d" % timeseriesId)
```

### Events

Learn more about events [here](https://doc.cognitedata.com/dev/concepts/resource_types/events.html)

```scala
// Scala Example. See Python example below.

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

```python
# Python Example

# Read events from `publicdata`
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "events") \
    .load()

# Insert the events in your own project using .save()
from pyspark.sql.functions import lit
df.withColumn("source", lit("publicdata")) \
    .write.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "events") \
    .option("onconflict", "abort") \
    .save()

# Get a reference to the events in your project
myProjectDf = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "events") \
    .load()
myProjectDf.createTempView("events")

# Update the description of all events from Open Industrial Data
spark.sql(
    "select 'Manually copied data from publicdata' as description," \
    " id," \
    " from events" \
    " where source = 'publicdata'") \
    .write.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("onconflict", "update") \
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

// Register your files in a temporary view
df.createTempView("files")


// Insert the files in your own project using .save()
spark.sql(s"""
      |select 'example-externalId' as externalId, 
      |'example-name' as name, 
      |'text' as source""")
  .write.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "files")
  .option("onconflict", "abort")
  .save()

//You can also insert using insertInto(). But you need to make sure the the schema is matched correctly.
 spark.sql(s"""
                |select "name-using-insertInto()" as name,
                |null as id,
                |'text' as source,
                |'externalId-using-insertInto()' as externalId,
                |null as mimeType,
                |null as metadata,
                |null as assetIds,
                |null as datasetId,
                |null as sourceCreatedTime,
                |null as sourceModifiedTime,
                |null as securityCategories,
                |null as uploaded,
                |null as createdTime,
                |null as lastUpdatedTime,
                |null as uploadedTime,
                |null as uploadUrl
     """.stripMargin)
      .select(df.columns.map(col):_*)
      .write
      .insertInto("files")
```

```python
# Python Example

# Read files metadata from publicdata
df = spark.read.format("cognite.spark.v1") \
  .option("apiKey", myApiKey) \
  .option("type", "files") \
  .load()

df.groupBy("fileType").count().show()

# Register your files in a temporary view
df.createTempView("files")

# Insert the files in your own project using .save()
spark.sql(
    "select 'example-externalId' as externalId," \
    " 'example-name' as name," \
    " 'text' as source") \
  .write.format("cognite.spark.v1") \
  .option("apiKey", "myApiKey") \
  .option("type", "files") \
  .option("onconflict", "abort") \
  .save()

# You can also insert data using insertInto(). But you need to make sure the the schema is matched correctly.
# The example using insertInto() is given above in Scala example.
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

```python
# Python Example

# Read 3D models metadata from a project with 3D models and revisions
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", "apiKeyToProjectWith3dModels") \
    .option("type", "3dmodels") \
    .load()

df.show()
```

### Sequences

Learn more about sequences [here](https://docs.cognite.com/dev/concepts/resource_types/sequences.html)

```scala
// Scala Example. See Python example below.

// List all sequences
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", myApiKey)
  .option("type", "sequences")
  .load()

// Create new sequence using Spark SQL
spark.sql("""
 |select 'c|$key' as externalId,
 |'c seq' as name,
 |'Sequence C detailed description' as description,
 |array(
 |  named_struct(
 |    'metadata', map('foo', 'bar', 'nothing', NULL),
 |    'name', 'column 1',
 |    'externalId', 'c_col1',
 |    'valueType', 'STRING'
 |  )
 |) as columns
""".stripMargin)
.write.format("cognite.spark.v1")
.option("apiKey", myApiKey)
.option("type", "sequences")
.option("onconflict", "abort")
.save()
```

```python
# Python Example

# List all sequences
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "sequences") \
    .load()

# Create new sequence using Spark SQL
spark.sql(
    "select 'c|$key' as externalId," \
    " 'c seq' as name," \
    " 'Sequence C detailed description' as description," \
    " array(" \
    "   named_struct(" \
    "     'metadata', map('foo', 'bar', 'nothing', NULL)," \
    "     'name', 'column 1'," \
    "     'externalId', 'c_col1'," \
    "     'valueType', 'STRING'" \
    "   )" \
    " ) as columns") \
    .write.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "sequences") \
    .option("onconflict", "abort") \
    .save()
```

### Sequence Rows

Learn more about sequences [here](https://docs.cognite.com/dev/concepts/resource_types/sequences.html)

One of two additional options must be specified:
* `id`: Cognite internal id of the sequence that is read or written to
* `externalId`: the external Id of the sequence that is read or written to

```scala
// Scala Example. See Python example below.

// Read sequence rows
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", myApiKey)
  .option("type", "sequencerows")
  .option("id", sequenceId) // or you can use "externalId" option
  .load()

// Insert the rows into another sequence using .save()
import org.apache.spark.sql.functions._
df
  .write.format("cognite.spark.v1")
  .option("apiKey", myApiKey)
  .option("type", "sequencerows")
  .option("onconflict", "upsert")
  .option("externalId", "my-sequence")
  .save()
```

```python
# Python Example

# Read sequence rows
df = spark.read.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "sequencerows") \
    .option("id", sequenceId) \
    .load()

# Insert the rows into another sequence using .save()
from pyspark.sql.functions import lit
df \
    .write.format("cognite.spark.v1") \
    .option("apiKey", myApiKey) \
    .option("type", "sequencerows") \
    .option("onconflict", "upsert") \
    .option("externalId", "my-sequence") \
    .save()
```

### RAW tables

Learn more about RAW tables [here](https://doc.cognitedata.com/api/v1/#tag/Raw).

RAW tables are organized in databases and tables that you need to provide as options to the DataFrameReader. `publicdata` does not contain any RAW tables so you'll need access to a project with raw table data.

Two additional options are required:

- `database`: The name of the database in Cognite Data Fusion's RAW storage to use. The database must exist, and will not be created if it does not.

- `table`: The name of the table in Cognite Data Fusion's RAW storage to use. The table must exist in the database specified in the `database` option, and will not be created if it does not.

Optionally, you can have Spark infer the DataFrame schema with the following options:

- `inferSchema`: Set this to `"true"` to enable schema inference. You can also use the inferred schema can also be used for inserting new rows.

- `inferSchemaLimit`: The number of rows to use for inferring the schema of the table. The default is to read all rows.

- `collectSchemaInferenceMetrics`: Whether metrics should be collected about the read operations for schema inference.

```scala
val df = spark.read.format("cognite.spark.v1")
  .option("apiKey", "myApiKey")
  .option("type", "raw")
  .option("database", "database-name") // a RAW database from your project
  .option("table", "table-name") // name of a table in "database-name"
  .load()
df.createTempView("tablename")

// Insert some new values
spark.sql("""insert into tablename values ("key", "values")""")
```

```python
# Python Example

# database-name -> a RAW database from your project
# table-name -> name of a table in "database-name"
df = spark.read.format("cognite.spark.v1") \
  .option("apiKey", myApiKey) \
  .option("type", "raw") \
  .option("database", "database-name") \
  .option("table", "table-name") \
  .load()

df.createTempView("tablename")

# Insert some new values
spark.sql("insert into tablename values ('key', 'values')")
```


## Build the project with sbt

The project runs read-only integration tests against the Open Industrial Data project. Navigate to
https://openindustrialdata.com/ to get an API key and store it in the `TEST_API_KEY_READ` environment variable.

To run the write integration tests, you'll also need to set the `TEST_API_KEY_WRITE` environment variable to an API key for a project where you have write access.

For Cognite employees: To run tests against greenfield, set the `TEST_API_KEY_GREENFIELD` environment variable
to an API key with read access to the project cdp-spark-datasource-test.

If you are using the SBT shell in IntelliJ or similar and want to get it to pick up environment variables
from a file, you can create a file in this directory named `.env` containing environment variables, one
per line, of the format `ENVIRONMENT_VARIABLE_NAME=value`.
See [sbt-dotenv](https://github.com/mefellows/sbt-dotenv) for more information.

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

Get an API-key for the Open Industrial Data project at https://openindustrialdata.com and run the following commands (replace \<release\> with the release you'd like, for example 1.2.0):

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
