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

## Short-term list of missing features:

- Multi-tags (finally figured out how to best do it)
- implement logging according to the standard spark way
- streaming support
- figure out how to expose metadata
- figure out how to expose aggregates in a good way
