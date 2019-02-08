# Spark data source for CDP API

Supports read and write for raw and clean data types.
Raw tables will be read in parallel. Writes to all types are done in parallel
through asynchronous writes.

## Build the project with sbt:

The project runs some tests against the jetfiretest2 project, to make them pass set the environment
variable `TEST_API_KEY` to an API key with access to the `jetfiretest2` project.

For more information about Jetfire see https://cognitedata.atlassian.net/wiki/spaces/cybertron/pages/575602824/Jetfire 
and https://docs.google.com/presentation/d/11oM_Z-NbFAl-ULOvBzG6YmSVRYbPCDuFOnjLed8IuwM
or go to https://jetfire.cogniteapp.com/ to try it out.

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
  .option("project", "publicdata")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "assets")
  .load()

df: org.apache.spark.sql.DataFrame = [name: string, parentId: bigint ... 3 more fields]

scala> df.count
res0: Long = 1000
```

## Reading from, and writing to, a Datasource:

cdp-spark-datasource supports reads from, and writes to, assets, timeseries, tables, datapoints and events.

To read from the different Datasources you need to provide three things:
an API-key, a project name and the Datasource type. To read from a table you should also specify the database name and table name.

To write to a Datasource you'll need to register a DataFrame that was read from
that Datasource as a temporary view. Since Open Industrial Data is read only you'll
need a project where you have write access and replace `desinationProject` and `destApiKey`
in the examples below.

### Assets

```
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "publicdata")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "assets")
  .load()
  
val destinationDf = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", <destinationProject>)
  .option("apiKey", <destApiKey>)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "assets")
  .load()

destinationDf.createTempView("destination")

df.select(destinationDf.columns.map(col):_*)
.write
.insertInto("destination")
```

### Timeseries

```
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "publicdata")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "timeseries")
  .load()
  
val destinationDf = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", <destinationProject>)
  .option("apiKey", <destApiKey>)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "timeseries")
  .load()

destinationDf.createTempView("destination")

df.select(destinationDf.columns.map(col):_*)
.write
.insertInto("destination")
```

### Datapoints

```
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "publicdata")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "datapoints")
  .load()
  
val destinationDf = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", <destinationProject>)
  .option("apiKey", <destApiKey>)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "datapoints")
  .load()

destinationDf.createTempView("destination")

df.select(destinationDf.columns.map(col):_*)
.write
.insertInto("destination")
```

### Events

```
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "publicdata")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "events")
  .load()
  
val destinationDf = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", <destinationProject>)
  .option("apiKey", <destApiKey>)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "events")
  .load()

destinationDf.createTempView("destination")

df.select(destinationDf.columns.map(col):_*)
.write
.insertInto("destination")
```

### Raw table

Since the Open Industrial Data publicdata project does not contain any tables we will use the playground project.
```
val apiKey = <playground-api-key>
val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "playground")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "tables")
  .option("database","workorders")
  .option("table","workorders1")
  .load()
  
val destinationDf = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", <destinationProject>)
  .option("apiKey", <destApiKey>)
  .option("batchSize", "1000")
  .option("limit", "1000")
  .option("type", "tables")
  .option("database",<targetDB>)
  .option("table",<targetTable>)  
  .load()

destinationDf.createTempView("destination")

df.select(destinationDf.columns.map(col):_*)
.write
.insertInto("destination")
```


## Short-term list of missing features:

- Multi-tags (finally figured out how to best do it)
- implement logging according to the standard spark way
- streaming support
- figure out how to expose metadata
- figure out how to expose aggregates in a good way
