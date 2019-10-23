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
* Filters are now pushed to CDF when possible for assets, events, files and raw tables.

* Raw Tables now expose a `lastUpdatedTime` column, and filters for it are pushed to CDF.

* Better error messages for invalid `onConflict` options.

* An error is now thrown if trying to update with null as id.

## Fixes
* Infer schema limit for Raw is now being used again.

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
* Validation of `key` column for raw tables, null values are not allowed.

## Enhancements
* Improved performance for assets.
* Retries on error code 500 responses.
* New `maxRetries` option for all resource types to set the number of retries.
* Improved back off algorithm for retries.
* `project` is no longer a necessary option, it will be retrieved from the API key.
