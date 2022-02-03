package cognite.spark.cdfdump

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.time.DurationFormatUtils
import org.log4s._
import org.rogach.scallop._

import java.nio.file.{Files, Paths}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  noshort = true
  appendDefaultToDescription = true
  helpWidth(130)
  version(s"cdf_dump version ${BuildInfo.version}.\n" +
    s"Running on Apache Spark ${org.apache.spark.SPARK_VERSION}\n" +
    s"${util.Properties.versionMsg}\n" +
    s"Built with sbt ${BuildInfo.sbtVersion}\n")

  banner("""
           |cdf_dump is a CLI wrapper for Cognite Spark Data Source for dumping data from CDF.
           |Best suited for dumping largish amount of data, for smaller datasets you might be better off using the Python SDK.
           |
           |Usage:
           |  * Raw table
           |      cdf_dump --raw my-db.table -o path/to/dumped/data
           |
           |  * Assets and events into the default directory
           |      cdf_dump --assets --events
           |  * Assets, but only with a specific external id prefix
           |      cdf_dump --assets --where 'externalId LIKE "my-prefix-%"'
           |
           |Authentication:
           |  Done using environment variables, there are these options:
           |  * api key: set COGNITE_API_KEY
           |  * OIDC auth: set COGNITE_TOKEN_URL, COGNITE_CLIENT_ID, COGNITE_CLIENT_SECRET, and COGNITE_PROJECT. Optionally also COGNITE_SCOPES if it's not $baseUrl/.default
           |  * Bearer token: set COGNITE_BEARER_TOKEN, and COGNITE_PROJECT. Note that bearer tokens have limited validity, so the dump process might not finish before the token times out.
           |
           |  COGNITE_BASE_URL may be used for accessing different clusters than api.cognitedata.com.
           |
           |  To intercept traffic with mitmproxy, run `mitmproxy --mode reverse:https://api.cognitedata.com -p 4001` and set COGNITE_BASE_URL = http://localhost:4001
           |
           |Options:
           |""".stripMargin)

  val raw = opt[List[String]]("raw", argName = "db.table", descr = "Download the selected raw tables.", default = Some(List.empty))
  val assets = opt[Boolean]("assets", descr = "Download all assets.")
  val events = opt[Boolean]("events", descr = "Download all events.")
  val timeseries = opt[Boolean]("timeseries", descr = "Download all timeseries metadata.")
  val relationships = opt[Boolean]("relationships", descr = "Download all relationships.")
  val files = opt[Boolean]("files", descr = "Download all file metadata.")
  val sequences = opt[Boolean]("sequences", descr = "Download all sequences metadata.")
  val labels = opt[Boolean]("labels", descr = "Download all label metadata.")
  val datasets = opt[Boolean]("datasets", descr = "Download all dataset metadata.")

  val allClean = opt[Boolean]("all-clean", descr = "Download assets, events, timeseries, relationships, files, sequences, labels and datasets.")
  group("Which CDF data to load?").append(
    raw, assets, events, timeseries, relationships, files, sequences, labels, datasets, allClean
  )


  val filter = opt[String]("where", 'w', "Spark SQL where filter. Supports filter pushdown as described in https://github.com/cognitedata/cdp-spark-datasource/#filter-pushdown")

  val columns = opt[List[String]]("columns", argName = "expression AS columnName", descr = "Which columns to include. Support Spark SQL expression, so use `metadata.tag as tag` to extract metadata into flat table.")
  val excludeColumn = opt[List[String]]("exclude-column", argName = "column", descr = "Which columns to exclude. Might be useful to exclude metadata and other columns which are not supported in CSV.")
  group("Basic data processing").append(filter, columns, excludeColumn)

  val outDir = opt[String]("out-dir", 'o', noshort = false, argName = "path", descr = "Output directory. By default `out` in the current working directory.", default = Some("out"))
  val format = opt[String]("format", 'f', noshort = false, argName = "conditionExpression", descr =  "Output format supported by Spark. Use json [default], csv, parquet, orc", default = Some("json"))
  val clearOutDir = opt[Boolean]("clear-out-dir", descr = "When set, all items will be removed from the output directory before the process starts.", default = Some(false))
  group("Basic output options").append(outDir, format, clearOutDir)

  val readOptions = props[String]('R', "Spark read options. You can use any option supported by CDF Spark Data Source, see: https://github.com/cognitedata/cdp-spark-datasource/#common-options")
  val writeOptions = props[String]('W', "Spark write options. You can use any option supported in your selected output format. For example `-f csv -WincludeHeader=true` to write CSV with headers.")
  val sparkConfig = props[String]('S', "Spark config option. You can use any property listed in the Spark Docs: https://spark.apache.org/docs/latest/configuration.html#available-properties")

  val outputPartitions = opt[Int]("out-partitions", argName = "number", descr = "Number of output partitions for text formats. By default 1, increase for better performance, but fragmented output.", default = Some(1))
  val maxRetries = opt[Int]("max-retries", argName = "number", descr = "Number of retries to do when CDF request fails. By default it's quite high (about 10) and we use exponential backoff. Set to 0 for debugging, to improve responsibility.")
  group("Advanced Spark options").append(readOptions, writeOptions, outputPartitions, maxRetries)

  verify()
}

object Main extends App {
  val logger = getLogger
  val a = new Conf(args)

  val outDir = a.outDir()

  if (a.clearOutDir()) {
    if (Files.exists(Paths.get(outDir))) {
      if (Files.isDirectory(Paths.get(outDir))) {
        logger.info(s"Removing all items in $outDir")
        Files.list(Paths.get(outDir)).forEach(f => FileUtils.deleteQuietly(f.toFile))
      } else {
        logger.info(s"Removing file $outDir")
        Files.delete(Paths.get(outDir))
      }
    }
  }

  lazy val helper = new SparkHelper(
    a.sparkConfig,
    a.writeOptions,
    a.readOptions,
    outDir,
    a.format(),
    a.columns.toOption,
    a.excludeColumn.getOrElse(List.empty),
    a.filter.toOption,
    a.outputPartitions.toOption,
    a.maxRetries.toOption
  )

  a.raw().foreach(table => {
    val Array(db, t) = table.split("[.]", 2)
    log(s"Raw table $db.$t", helper.saveRaw(db, t))
  })

  if (a.assets() || a.allClean()) {
    log("Assets", helper.saveClean("assets"))
  }

  if (a.events() || a.allClean()) {
    log("Events", helper.saveClean("events"))
  }

  if (a.timeseries() || a.allClean()) {
    log("Time series", helper.saveClean("timeseries"))
  }

  if (a.relationships() || a.allClean()) {
    log("Relationships", helper.saveClean("relationships"))
  }

  if (a.files() || a.allClean()) {
    log("Files", helper.saveClean("files"))
  }

  if (a.sequences() || a.allClean()) {
    log("Sequences", helper.saveClean("sequences"))
  }

  if (a.labels() || a.allClean()) {
    log("Labels", helper.saveClean("labels"))
  }

  if (a.datasets() || a.allClean()) {
    log("Datasets", helper.saveClean("datasets"))
  }

  def log(c: String, action: => Unit): Unit = {
    println(s"Downloading $c")
    val t0 = System.nanoTime()
    action
    val t1 = System.nanoTime()
    val time = (t1 - t0) / 1000 / 1000
    //val time = java.time.Duration.ofNanos(t1 - t0)
    // println(s"Loaded $c in ${DurationFormatUtils.formatDurationWords(time, true, true)}")
    println(s"Loaded $c in ${DurationFormatUtils.formatDurationHMS(time)}")
  }
}
