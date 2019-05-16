# 0.4.9

## Fixes

* Speed up time series and events by avoiding unions

## Enhancements
* Support Scala 2.12

* New write mode using `.save()` allows specifying behaviour on conflicts

* Partial updates now possible for assets, events and time series

* Assets now support asset types

* Bearer tokens can now be used for authentication

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
