package com.yotabites.utils

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait Metastore {

  def result2Metric(result: Any, metricCf: String): Metric
  def putMetrics(spark: SparkSession, metricObj: Metric, config: Config): Boolean
  def getLatestCheckpoint(spark: SparkSession, config: Config, project: String): Long
}
