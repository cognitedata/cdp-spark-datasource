# 0.4.4

## Bugfixes
* Bugfix for `datapoints` writes.


# 0.4.3

## Bugfixes
* Fixed dependencies in .jar, removed "fat" jar from release.


# 0.4.2

## Bugfixes
* Bugfix for `3dmodelrevisionmappings` (treeIndex and subtreeSize are optional).

## Enhancements
* `baseUrl` option to use a different prefix than https://api.cognitedata.com for all Cognite Data Platform API calls.


# 0.4.1

## Enhancements
* Read-only support for files metadata.
* Initial read-only support for 3D data (should be considered an *alpha feature, may not work*).


# 0.4.0

## Breaking changes
* *Breaking change* `"tables"` renamed to `"raw"`.

## Bugfixes
* Validation of `key` column for raw tables, null values are not allowed.

## Enhancements
* Improved performance for assets.
* Retries on error code 500 responses.
* New `maxRetries` option for all resource types to set the number of retries.
* Improved back off algorithm for retries.
* `project` is no longer a necessary option, it will be retrieved from the API key.
