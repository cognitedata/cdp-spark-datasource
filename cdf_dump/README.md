# cdf_dump CLI tool

The cdf_dump utility is a simple CLI wrapper for [Cognite Spark Data Source](https://github.com/cognitedata/cdp-spark-datasource)
to simplify copying of large amounts data from CDF.

## Installation

Download the cdf_dump.jar from [Github Releases](https://github.com/cognitedata/cdp-spark-datasource/releases) and run it from command line: `java -jar cdf_dump.jar`.
The cdf_dump.jar file contains all dependencies, including Apache Spark, so it's bit large, but should not require anything except the JVM on your OS.
Spark currently only supports Java 11 and Java 8, so cdf_dump also won't work on other JVM versions.
If you have the choice, we recommend Java 11. You may use GraalVM, but it doesn't seem to improve performance much.


## Examples

```shell
cdf_dump --raw my-db.table -o out_directory
```

Copies `my-db.table` into `out_directory/raw/my-db/table` directory in JSON format. You can use [`jq`](https://stedolan.github.io/jq/) to process the data further.

```shell
cdf_dump --assets --events
```

Copies all assets and events from the project (all visible to the logged in user) into `./out/assets` and `./out/events` directories.
`./out` is the default output (`-o`) and all directories are created automatically.
However, it will not overwrite data in the directory unless you specifically use the `--clear-out-dir` option.

```shell
cdf_dump --all-clean
```

Copies all clean resources to the out directory. Includes events, assets, timeseries metadata, file metadata, sequences data ... Does not include RAW, timeseries datapoints, file data, sequences data.


```shell
cdf_dump --raw my-db.table -f parquet
```

Outputs data in the [Apache Parquet format](https://parquet.apache.org/), typically offers better performance than JSON or CSV.
You can use [DuckDB](https://duckdb.org/docs/data/parquet) to process the data locally.

```shell
cdf_dump --assets --where 'externalId LIKE "my-prefix-%" or dataSetId = 1234'
```

Load assets with a specific external id prefix. We pushdown certain filters to CDF, see [which filters are supported](https://github.com/cognitedata/cdp-spark-datasource#schemas).
The `--where` option can contain anything you'd put in SQL `WHERE` clause, but if the filter is not supported, it will be evaluated locally after the data is downloaded.

```shell
cdf_dump --assets -RbatchSize=10
```

Read assets in batches of 10. The `-R` option may be used to specify any Spark option for the CDF reader. See the [list of available options](https://github.com/cognitedata/cdp-spark-datasource#common-options) section for more details. Similarly `-W` may be used for write options.


```shell
cdf_dump --raw my.table -RparallelismPerPartition=5
```

Change the default of 20 of parallelism per (Spark) partition to 5. Note that number of Spark partitions = `partitions` / `parallelismPerPartition` and defaults are 200 and 20.
Changing these options is somewhat advanced, but may be necessary for very large datasets, since one Spark partition should fit into RAM.
Increasing `partitions` or lowering `parallelismPerPartition` may thus help with memory consumption.


```shell
cdf_dump --relationships -Sspark.ui.enabled=false
```

Dump all relationships with the Spark UI disabled.
Spark UI is useful for debugging, but might be unwanted if you would deploy a script using `cdf_dump`.
The `-S` option may be used for any set of [Spark configuration properties](https://spark.apache.org/docs/latest/configuration.html#available-properties).

## Authentication

Authentication is done using environment variables, there are these options:

* api key: set `COGNITE_API_KEY`
* Open ID Connect: set `COGNITE_TOKEN_URL`, `COGNITE_CLIENT_ID`, `COGNITE_CLIENT_SECRET`, and `COGNITE_PROJECT`.
  Optionally also `COGNITE_SCOPES` if it's not equal to `$baseUrl/.default` and `COGNITE_AUDIENCE` when needed.
* Bearer token: set `COGNITE_BEARER_TOKEN`, and `COGNITE_PROJECT`. Note that bearer tokens have limited validity, so the dump process might not finish before the token times out.

`COGNITE_BASE_URL` may be used for accessing different clusters than `https://api.cognitedata.com`.

For debugging: to intercept traffic with [mitmproxy](https://docs.mitmproxy.org/stable/), run `mitmproxy --mode reverse:https://api.cognitedata.com -p 4001` and set `COGNITE_BASE_URL = http://localhost:4001`

## Help message

`--help` will print this message (copied here so Google can index it and so you don't need to download the tool to check that it does not support what you need):

```
cdf_dump is a CLI wrapper for Cognite Spark Data Source for dumping data from CDF.
Best suited for dumping largish amount of data, for smaller datasets you might be better off using the Python SDK.

Usage:
  * Raw table
      cdf_dump --raw my-db.table -o path/to/dumped/data

  * Assets and events into the default directory (./out)
      cdf_dump --assets --events
  * Assets, but only with a specific external id prefix
      cdf_dump --assets --where 'externalId LIKE "my-prefix-%"'

Authentication:
  Done using environment variables, there are these options:
  * api key: set COGNITE_API_KEY
  * OIDC auth: set COGNITE_TOKEN_URL, COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET, and COGNITE_PROJECT. Optionally also COGNITE_SCOPES if it's not $baseUrl/.default
  * Bearer token: set COGNITE_BEARER_TOKEN, and COGNITE_PROJECT. Note that bearer tokens have limited validity, so the dump process might not finish before the token times out.

  COGNITE_BASE_URL may be used for accessing different clusters than api.cognitedata.com.

  To intercept traffic with mitmproxy, run `mitmproxy --mode reverse:https://api.cognitedata.com -p 4001` and set COGNITE_BASE_URL = http://localhost:4001

Options:

 Which CDF data to load?
      --raw  <db.table>...                       Download the selected raw tables. (default = List())
      --assets                                   Download all assets.
      --events                                   Download all events.
      --timeseries                               Download all timeseries metadata.
      --relationships                            Download all relationships.
      --files                                    Download all file metadata.
      --sequences                                Download all sequences metadata.
      --labels                                   Download all label metadata.
      --datasets                                 Download all dataset metadata.
      --all-clean                                Download assets, events, timeseries, relationships, files, sequences, labels and
                                                 datasets.

 Basic data processing
      --where  <arg>                             Spark SQL where filter. Supports filter pushdown as described in
                                                 https://github.com/cognitedata/cdp-spark-datasource/#filter-pushdown
      --columns  <expression AS columnName>...   Which columns to include. Support Spark SQL expression, so use `metadata.tag as
                                                 tag` to extract metadata into flat table.
      --exclude-column  <column>...              Which columns to exclude. Might be useful to exclude metadata and other columns
                                                 which are not supported in CSV.

 Basic output options
  -o, --out-dir  <path>                          Output directory. By default `out` in the current working directory.
                                                 (default = out)
  -f, --format  <conditionExpression>            Output format supported by Spark. Use json [default], csv, parquet, orc
                                                 (default = json)
      --clear-out-dir                            When set, all items will be removed from the output directory before the process
                                                 starts.

 Advanced Spark options
  -Rkey=value [key=value]...                     Spark read options. You can use any option supported by CDF Spark Data Source,
                                                 see: https://github.com/cognitedata/cdp-spark-datasource/#common-options
  -Wkey=value [key=value]...                     Spark write options. You can use any option supported in your selected output
                                                 format. For example `-f csv -WincludeHeader=true` to write CSV with headers.
      --out-partitions  <number>                 Number of output partitions for text formats. By default 1, increase for better
                                                 performance, but fragmented output. (default = 1)
      --max-retries  <number>                    Number of retries to do when CDF request fails. By default it's quite high (about
                                                 10) and we use exponential backoff. Set to 0 for debugging, to improve
                                                 responsibility.

  -Skey=value [key=value]...                     Spark config option. You can use any property listed in the Spark Docs:
                                                 https://spark.apache.org/docs/latest/configuration.html#available-properties
  -h, --help                                     Show help message
  -v, --version                                  Show version of this program
```

## Build from sources

We use SBT compile and run the project. The following command builds the project and produces the `cdf_dump/target/scala-2.12/cdf_dump.jar` file.

```shell
sbt "cdf_dump/assembly"
```

For debugging you can only run `sbt "cdf_dump/run --help"`, which builds and immediately runs the tool.

## Troubleshooting

* `cdf_dump` run for few minutes and then complains about 401 - unauthorized.
	- You probably forgot about some authentication environment variables.
	  It runs for few minutes to figure this out because it does many retries to alleviate the risk that CDF has a short downtime.
	  For debugging auth, we recommend to use the `--max-retries 0` option to disable this behavior.

* A weird dependency error occurs
	- Sometimes sbt/assembly breaks things since there are many overlapping dependencies, try to run the same command using `sbt "cdf_dump/run ...options`, if that succeeds we are missing some cases in `assembly / assemblyMergeStrategy` option.

* It runs for a long time and it's not clear what's happening.
	- Use the Spark UI at `http://localhost:4040` to gain some insight.
	  For example, it will tell you how many rows did it process.
	  If you are seeing bad read performance, please file a bug.
	  If you are the developer, you can use [Java async-profiler](https://github.com/jvm-profiling-tools/async-profiler): `async-profiler -d 60 -f flamegraph.html <cdf_dump PID>`

* The `file:.../out/raw/test/table already exists.` error is annoying
	- Use the `--clear-out-dir` to remove everything from the output directory before running.
