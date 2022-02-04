# 1.4.62

## Fixes

* Chunk sequences when creating to avoid limit 10000 columns

# 1.4.61

## Fixes

* Handle empty string for Byte, Short, Integer and Long type in RawJsonConverter

# 1.4.60

## Enhancements

* Add support delete by externalIds for Assets, Events and TimeSeries

# 1.4.59

## Fixes

* Handle empty string for Boolean type in RawJsonConverter

# 1.4.58

## Fixes

* Handle empty string for Double and Float type in RawJsonConverter

# 1.4.57

## Enhancements

* Refactor JSON processing in RAW read to significantly improve read performance (especially large improvement are expected for wide tables)

# 1.4.56

## Enhancements

* Add support for parallel retrival of files, and filter pushdown on files
* General improvements to the filter pushdown logic, added filter pushdown support for a few minor fields

# 1.4.54

## Fixes

* Fix databricks issue caused by version conflict when spark upgrade

# 1.4.53

## Enhancements

* Add baseUrl into OAuth2.Session

# 1.4.52

## Fixes

* Handling null in toRawJson function.

# 1.4.51

## Enhancements

* Removed usage of Jackson from Raw writes. This should reduce dependency conflicts and improve performance.

# 1.4.50

## Enhancements

* Using onconflict=delete on assethierarchy will delete assets recursively 

# 1.4.49

## Enhancements

* Better handling of rate limiting from CDF - Spark data source will now wait with all other requests when rate limiting response (429 or 503) is encountered.
* Improve datapoints read performance.

## Fixes

* Fixed metrics for request status codes.

# 1.4.48

## Fixes

* Reverse order to give session more priority than client credential.

# 1.4.47

## Fixes

* Add the new field `sessionId` to session as it is required when refreshing the session.

# 1.4.46

## Fixes

* Fixed a bug where inserting into many timeseries created too many small requests

# 1.4.45

## Enhancements

* Drop scala 2.11 and support scala 2.13

# 1.4.44

## Fixes

* Fixed a bug where filtering on many RAW columns would create too long URL requests.

# 1.4.43

## Fixes

* Specify timeStamp for upper bound when retrieving dataPoints.

# 1.4.42

## Enhancements

* No column filter will be pushed to RAW tables when selecting all columns.

# 1.4.41

## Enhancements

* RAW tables will now be filtered in the API when selecting individual columns by name.

# 1.4.40

## Enhancements

* Support filter pushdown on externalIdPrefix (for example using SQL `externalId LIKE 'Something%'`). Assets, Time Series and Events support it.

# 1.4.39

## Enhancements

* Update to Spark version 3.1.2.

# 1.4.38

## Fixes

* Use scala-sdk 1.5.14, with stable token/inspect support.

## Enhancements

* Avoid fetching all items when filtering by Id or ExternalId.

# 1.4.37

## Enhancements

* Retry requests on connection and read errors.

# 1.4.36

## Enhancements

* Support updating & upserting a `Relationship` by its `externalId`

# 1.4.35

## Fixes

* Fixed reading of datapoints with many partitions (see #523 for more details)

# 1.4.34

## Fixes

* `set` method is now used for `labels` updates.

# 1.4.33

## Enhancements

* `datasets` have been added as a new resource type. See [Data sets](README.md#data-sets)
  for more information.

* `Sequences` are now read in parallel.

# 1.4.32

## Enhancements

* `Files` relation now supports `directory` property.

# 1.4.31

## Fixes

* Fixed performance problem with very large asset hierarchies

# 1.4.30

## Enhancements

* Limit the number to parallel request to CDF. Should avoid getting rate limiting errors.
* Add metrics for number of request going to CDF
* Added `audience` option for Aize and AKSO OAuth2 flow. Removed default value for `scope` to support audience.

## Fixes

* Fixed default batchSize for deleting datapoints

# 1.4.29

## Enhancements

* Added `rawEnsureParent` option that allows creating the raw table when writing RAW rows.

# 1.4.28

## Enhancements

* Improves the performance and stability of the asset hierarchy builder.

## Fixes

* Corrected some mistakes from 1.4.27 that were causing data points read to hang indefinitely.

# 1.4.27

*NOTE*: This update has some problems related to JDK-version, do not use.

## Enhancements

* Security update of various internal libraries and the Scala SDK.

# 1.4.26

## Fixes

* Made the `ignoreNullFields` option also work in upsert mode.

# 1.4.25

## Enhancements

* Added `ignoreNullFields` option. Useful for nulling fields in CDF.

# 1.4.24

## Fixes

* Fix leaking threads when using OIDC credentials.

# 1.4.23

## Enhancements

* Added `authTicket` option for ticket auth.

# 1.4.22

## Enhancements
* SequenceRows are now written to one or multiple sequences by refering each rows `externalId`.

* Now uses the 1.4.6 version of the Scala SDK.

# 1.4.21

## Enhancements
* Now uses the 1.4.5 version of the Scala SDK.

# 1.4.20

## Enhancements
* Native tokens can now be used for authentication.

# 1.4.19

## Enhancements
* New exception type `CdfInternalSparkException` has been introduced.

# 1.4.18

## Fixes
* Stop reading from CDF immediately on task completion/cancellation.
This will allow Spark to start processing other tasks more quickly, especially when
there are exceptions thrown by tasks.

# 1.4.17

## Fixes
* Handle additional uncaught exceptions locally, instead of having them kill the executor.

# 1.4.16

## Fixes
* Handle some uncaught exceptions locally, instead of having them kill the executor.

# 1.4.15

## Enhancements
* The `labels` field is now available for assets on update and upsert operations.
* The `labels` field is now available for asset hierarchy builder on upsert operation.

# 1.4.14

## Enhancements
* Set max retry delay on requests to 30 seconds by default, configurable via
`maxRetryDelay` option.

## Fixes
* Fix a potential deadlock in handling exceptions when reading and writing data from CDF.

# 1.4.13

## Enhancements
* `relationships` have been added as a new resource type. See [relationships](README.md#relationships)
for more information.
* The `labels` field is now available for assets on read and insert operations.

# 1.4.12

## Enhancements
* Spark 3 is now supported!
* `labels` have been added as a new resource type. See [Labels](README.md#labels)
for more information.

# 1.4.11

## Fixes

* Fix a bug where certain operations would throw a `MatchError` instead of the intended exception type.

# 1.4.10

## Enhancements
* Improved error message when attempting to use the asset hierarchy builder to move an asset between different root assets.

# 1.4.9

## Enhancements
* Upgrade to Cognite Scala SDK 1.4.1
* Throw a more helpful error message when attempting to use sequences that contain columns without an externalId.

# 1.4.8

## Fixes
* Attempting an update without specifying either `id` or `externalId` will now result in a `CdfSparkException` instead of an `IllegalArgumentException`.

# 1.4.7

## Enhancements
* The `X-CDP-App` and `X-CDP-ClientTag` headers can now be configured using the `applicationName` and `clientTag` options.
  See the [Common Options](README.md#common-options) section for more info.

## Fixes
* Nested rows/structs are now correctly encoded as plain JSON objects when writing to RAW tables.
  These were previously encoded according to the internal structure of `org.apache.spark.sql.Row`.

# 1.4.6

## Fixes
* Use the configured batch size also when using savemode

# 1.4.5

## Fixes
* Make all exceptions be custom exceptions with common base type.

# 1.4.4

## Fixes
* Excludes the netty-transport-native-epoll dependency, which isn't handled
correctly by Spark's --packages support.

# 1.4.3

## Fixes
* Still too many dependencies excluded. Please use 1.4.4 instead.

# 1.4.2

## Enhancements
* Clean up dependencies to avoid evictions.
This resolves issues on Databricks where some evicted dependencies were loaded,
which were incompatible with the versions of the dependencies that should have
been used.

# 1.4.1

We excluded too many dependencies in this release. Please use 1.4.2 instead.

## Enhancements
* Clean up dependencies to avoid evictions.

# 1.4.0

## Breaking changes

* Metadata values are no longer silently truncated to 512 characters.

# 1.3.1

## Enhancements

* Deletes are now supported for `datapoints`. See README.md for examples.

## Fixes

* An incorrect version was used for one of the library dependencies.

# 1.3.0

## Breaking changes

Although not breaking for most users, this release updates some core
dependencies to new major releases. In particular, it is therefore
not possible to load 1.3.x releases at the same time as 0.4.x releases.

## Enhancements

* Sequences are now supported, see README.md for examples using
`sequences` and `sequencerows`.

* Files now support upsert, delete, and several new fields like
`dataSetId` have been added.

* Files now supports parallel retrieval.

# 1.2.20

## Enhancements

* Improved error message when a column has a incorrect type

## Fixes

* Filter pushdown can now handle null values in cases like `p in (NULL, 1, 2)`.
* Asset hierarchy now handles duplicated root parentExternalId.
* NULL fields in metadata are ignored for all resource types.

# 1.2.19

## Enhancements

* Improve data points read performance, concurrently reading different time
ranges and streaming the results to Spark as the data is received.

# 1.2.18

## Enhancements

* GZip compression is enabled for all requests.

## Fixes
* "name" is now optional for upserts on assets when external id is
specified and the asset already exists.

* More efficient usage of threads.

# 1.2.17

## Fixes
* Reimplement draining the read queue on a separate thread pool.

# 1.2.16

## Breaking changes
* Include the latest data point when reading aggregates. Please note that this is a breaking change
and that updating to this version  may change the result of reading aggregated data points.


## Enhancements
* Data points are now written in batches of 100,000 rather than 1,000.

* The error messages thrown when one or more columns don't match will
now say which columns have the wrong type.

* Time series delete now supports the `ignoreUnknownIds` option.

* Assets now include `parentExternalId`.

## Fixes
* Schema for RAW tables will now correctly be inferred from the first 1,000 rows.

* Release threads from the threadpool when they are no longer going to be used.

# 1.2.15

## Fixes

* Fixes a bug where not all data points would be read if a time series had less than 10,000 data points
per 300 days.

# 1.2.14

## Enhancements
* `dataSetId` can now be set for asset hierarchies.

* Metrics are now reported for deletes.

## Fixes

* Empty updates of assets, events, or time series no longer cause errors.

# 1.2.13

## Enhancements
* `assethierarchy` now supports metrics.

## Fixes
* Upserts are now supported when using `.option("useLegacyName", "externalId")`.

# 1.2.12

## Enhancements
* `dataSetId` can now be set for events, assets, and time series.

# 1.2.11

## Enhancements
* The `useLegacyName` option now supports setting `legacyName` based on `externalId`.
    Use `.option("useLegacyName", "externalId")` to enable this.

* A new option `project` allows the user to specify the CDF project to use.
If omitted, the project will be fetched using the `apiKey` or `bearerToken` option.

* A new resource type `assethierarchy` is now supported, allowing you to create
asset hierarchies from Spark data frames. See the README for more information.

# 1.2.10

## Enhancements
* Uses Cognite Scala SDK version 1.1.2, with further improved functionality to
retry requests.

# 1.2.9

## Fixes
* Fixes a bug where the aggregations `stepInterpolation`, `totalVariation`,
`continuousVariance` and `discreteVariance` could not be read due to case errors.

# 1.2.8

## Enhancements
* Java ConnectionException errors will now be retried, improving the robustness
of the Spark data source.

# 1.2.7

## Enhancements
* Multiple rows with the same `id` and `externalId` are now allowed for
upserts, but the order in which they are applied is undefined and we currently
only guarantee that at least one upsert will be made for each `externalId`,
and at least one update will be made for each `id` set.
This is based on the assumption that upserts for the same `id` or `externalId`
will have the same values. If you have a use case where this is not the case,
please let us know.

## Fixes
* We now limit the number of threads being used for HTTP connections.
In some cases it was possible to use too many threads for HTTP connections,
and run out of ephemeral ports.

# 1.2.6

## Fixes
* The `useLegacyName` option for time series is now respected also when doing upserts.

# 1.2.5

## Enhancements
* Upserts can now be done by internal id.

* Metrics are now collected for inserts and updates.

* Added support for the time series fields `isString` and `isStep` when doing upserts.

## Fixes
* Fixed a bug where certain resources could not write to other tenants than the main CDF tenant.

# 1.2.4

## Fixes
* RAW tables now respects the `baseUrl` option for writes.

* String data points now respects the `baseUrl` option for writes.

# 1.2.3

## Enhancements
* Support new option `ignoreUnknownIds` for asset and event deletes.
Assets and events will ignore existing ids on deletes.
The default value is true. Use `.option("ignoreUnknownIds", "false")` to
revert to the old behavior, where the job will be aborted when an attempt
to delete an unknown id is made.

* Use Cognite Scala SDK version 1.1.0

## Fixes
* Fetch data points at the end of the available count aggregates, even if
they are not ready yet. This will fetch all data points even if the last
aggregates claim there are no data points available.
Some edge cases may still not have been properly addressed yet.

# 1.2.2

## Enhancements
* Use Cognite Scala SDK version 1.0.1

# 1.2.1

## Enhancements
* `datapoints` and `stringdatapoints` now supports save mode.

* Increased the default number of partitions from 20 to 200.

## Fixes
* `stringdatapoints` now correctly fetches all data points.

* Fixed a bug in pushdown implementation that would cause no filters to be pushed down when
combining filters on pushdown and non-pushdown fields.

* `datapoints` will no longer fail when aggregates aren't ready in CDF yet.

* `datapoints` should now retrieve all aggregates.
Previously it could miss some aggregates due to a rounding error.

# 1.2.0

*NOTE*: `stringdatapoints` only retrieves the first 100000 data points.
This will be fixed in the next release. `datapoints` is fixed in this release.

## Breaking changes
* `assets` resource type now has `rootId` and `aggregates` fields.

## Fixes
* `datapoints` will now retrieve all numerical data points again.

## Enhancements
* Use `maxRetries` option to allow configuration of the number of retries to attempt.

* `timeseries` now supports parallel retrieval.

* `timeseries` does filter pushdown for name, unit, isStep, and isString columns.

* `datapoints` uses count aggregates for improved performance when retrieving numerical data points.

# 1.1.0

## Breaking changes
* The library has been renamed to "cdf-spark-datasource" instead of "cdp-spark-datasource".

* `isString`, `isStep` and `unit` have been removed from the data points schema. They were only used for reads.

## Enhancements
* Failed requests will be retried when appropriate failures are detected.

* You can set `baseUrl` as in `.option("baseUrl", "https://greenfield.cognitedata.com")`

# 1.0.0
This release goes from using Cognite API version 0.5/0.6 to using [Cognite API v1](https://docs.cognite.com/api/v1/).
All reads from, and writes to, CDF now use the [Cognite Scala SDK](https://github.com/cognitedata/cognite-sdk-scala).

## Breaking changes
* All schemas updated to match API v1

## Enhancements
* Parallel retrieval is now a lot faster, and parallelity can be specified using the `partitions` option.`

* All datetime columns are now Timestamps rather than milliseconds since Epoch.

* Format has been shortened, for convenience: `.format("cognite.spark.v1")`.

* Filtering Time Series on `assetId` is now applied API-side.

# 0.4.13

## Fixes
Fixed a bug with time series upsert where `insertInto` would only work under special conditions.

# 0.4.12

## Enhancements
* Assets will now do upsert when the `source`+`sourceId`-pair exists.

* When filtering Events on Ids the filter is now applied API-side

* Filter pushdown with `AND` and `OR` clauses has been optimized.

## Fixes
* Metadata keys with null values are now removed, avoiding NullPointerExceptions from the API.

# 0.4.11

## Enhancements
* Filters are now pushed to CDF when possible for assets, events, files and RAW tables.

* RAW tables now expose a `lastUpdatedTime` column, and filters for it are pushed to CDF.

* Better error messages for invalid `onConflict` options.

* An error is now thrown if trying to update with null as id.

## Fixes
* Infer schema limit for RAW is now being used again.

# 0.4.10

## Enhancements
* Support for deleting time series, events and assets with `.save()`.

* Set `x-cdp-sdk` header for all API calls.

# 0.4.9

## Fixes

* Speed up time series and events by avoiding unions.

## Enhancements
* Support Scala 2.12.

* New write mode using `.save()` allows specifying behaviour on conflicts.

* Partial updates now possible for assets, events and time series.

* Assets now support asset types.

* Bearer tokens can now be used for authentication.

# 0.4.8

## Fixes
* Allow `createdTime` and `lastUpdatedTime` to be "null" on inserts.

* Allow time series id to be null on insert, and always attempt to create the time series
if id is null.

# 0.4.7

## Fixes
* Fix upserts on time series metadata with security categories.

* Improved error messages when upserts fail.

* Avoid registering the same Spark metric name more than once.

# 0.4.6

## Fixes
* Creating events works again.

* Metadata values are truncated to 512 characters, which is now the limit set by Cognite.

## Enhancements
* Filters on "type" and "subtype" columns of `events` will be used to retrieve only events of matching type and subtype.

* Parallel cursors are used for reading `events` and `assets`.

* String data points are now supported using the `stringdatapoints` resource type.

* First and last data points available will be used to set timestamp limits if not given,
improving the performance of `datapoints` parallelization for most use cases.

# 0.4.5

## Fixes
* Writes for non-data points resource types work again.

## Enhancements
* All fields for all resource types should be present. In particular, many asset fields were previously not included.

* Upsert is now supported for time series metadata, based on the time series id.

* `partitions` can be used to control the partitions created for the `datapoints` resource type.
The time interval will be split into the given number of partitions and fetched in parallel.


# 0.4.4

## Fixes
* `datapoints` writes work again.


# 0.4.3

## Fixes
* Fixed dependencies in .jar, removed "fat" jar from release.


# 0.4.2

## Fixes
* Fix for `3dmodelrevisionmappings` (treeIndex and subtreeSize are optional).

## Enhancements
* `baseUrl` option to use a different prefix than https://api.cognitedata.com for all Cognite Data Platform API calls.


# 0.4.1

## Enhancements
* Read-only support for files metadata.
* Initial read-only support for 3D data (should be considered an *alpha feature, may not work*).


# 0.4.0

## Breaking changes
* *Breaking change* `"tables"` renamed to `"raw"`.

## Fixes
* Validation of `key` column for RAW tables, null values are not allowed.

## Enhancements
* Improved performance for assets.
* Retries on error code 500 responses.
* New `maxRetries` option for all resource types to set the number of retries.
* Improved back off algorithm for retries.
* `project` is no longer a necessary option, it will be retrieved from the API key.
