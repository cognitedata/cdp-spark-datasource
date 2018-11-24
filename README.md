# Spark data source for CDP API

Supports read and write for raw and clean data types.
Raw tables will be read in parallel. Writes to all types are done in parallel
through asynchronous writes.

## How to use:

To use it with spark-shell:

```
$> spark-shell --jars ~/path-to-repo/target/cdp-spark-datasource-jar-with-dependencies.jar
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

scala> val apiKey="secret-key-you-have"
scala> val df = spark.sqlContext.read.format("com.cognite.spark.datasource")
  .option("project", "akerbp")
  .option("apiKey", apiKey)
  .option("batchSize", "1000")
  .option("limit", "10000")
  .option("tagId", ""00ADD0002/B1/5mMid")
  .load()
  .where("timestamp > 0 and timestamp < 1390902000001")

df: org.apache.spark.sql.DataFrame = [tagId: string, timestamp: bigint ... 1 more field]

scala> df.count()
res0: Long = 1000
```

## Why mvn test is failing

To make the tests pass, set the environment variable `TEST_API_KEY`
to an API key with access to the `jetfiretest2` project.

## So how to build it?

```mvn package```

will give you a jar, ```cdp-spark-datasource-jar-with-dependencies.jar```

## Short-term list of missing features:

- Multi-tags (finally figured out how to best do it)
- implement logging according to the standard spark way
- streaming support
- figure out how to expose metadata
- figure out how to expose aggregates in a good way
