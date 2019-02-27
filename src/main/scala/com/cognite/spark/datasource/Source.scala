package org.apache.spark.datasource

// Workaround to use Source since it is private to org.apache.spark
trait Source extends org.apache.spark.metrics.source.Source
