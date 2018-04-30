# Spark data source for CDP API

Very rough prototype to wrap the CDP API so that it looks like a spark
data source. This version is currently reverted to the prototype stage
again to make it possible to use it.

A lot of features are missing and there are multiple performance fixes
that should go in asap. Especially limiting is that the datasources
API does not expose partitions, which means that a single worker will
be responsible for loading the data from the API. By fixing the
partitions, we should be able to scale up downloads to whatever number
of workers configured.

**New version is imminent!**

## How to use:

To use it with spark-shell:

```
$> spark-shell --jars ~/path-to-repo/target/cdb-spark-connector-jar-with-dependencies.jar
Spark context Web UI available at http://192.168.20.102:4040
Spark context available as 'sc' (master = local[*], app id = local-1513307936323).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/
Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_131)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val apikey="secret-key-you-have"
scala> val df = spark.sqlContext.read.format("com.cognite.spark.connector")
  .option("project", "akerbp")
  .option("apiKey", apikey)
  .option("batchSize", "1000")
  .option("limit", "10000")
  .option("tagId", ""00ADD0002/B1/5mMid")
  .load()
  .where("timestamp > 0 and timestamp < 1390902000001")

df: org.apache.spark.sql.DataFrame = [tagId: string, timestamp: bigint ... 1 more field]

scala> df.count()
res0: Long = 1000
```

(the same should work for spark-submit, although you may have to
distribute the jar in a mesos style cluster or put it on hdfs - this
is not specific for our code)

## Why mvn test is failing

Currently, the tests run against production akerbp tags. This is
temporary (and only read) until we get a nice local test to run against.

To make the tests pass, set the environment variable `COGNITE_API_KEY`
to an API key with access to the production akerbp tags.
It is highly recommended that you use an API key with *read-only* access.

The goal is of course to avoid this.

## So how to build it?

```mvn -DskipTests package```

will give you a jar, ```cdp-spark-connector-jar-with-dependencies.jar```

## Short-term list of missing features:

- Multi-tags (finally figured out how to best do it)
- Functioning pushdown which allows .where() etc. - currently fixing bugs on that
- Better schema handling
- Infer schema automatically
- better error handling
- retry on error
- support magical date format (-2w)
- implement logging according to the standard spark way
- expose partitions to allow for multi-worker-downloads
- write support
- use protobuf apis for improved performance
- expose more than the timeseries API
- streaming support
- figure out how to expose metadata
- figure out how to expose aggregators in a logical way

