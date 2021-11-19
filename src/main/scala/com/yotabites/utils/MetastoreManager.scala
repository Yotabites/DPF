package com.yotabites.utils

import java.util

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

object MetastoreManager extends LazyLogging {

  def putMetrics(spark: SparkSession, metricObj: Metric, config: Config): Boolean = {
    val metastoreType = config.getString("metastore.type")
    val metastore =
      metastoreType match {
      case "hbase" => new HBaseMetastore
      case "delta" => new DeltaLakeMetastore
    }
    val result = metastore.putMetrics(spark, metricObj,config)
    result
  }

  def getLatestCheckpoint(spark: SparkSession, config: Config, project: String): Long = {
    val metastoreType = config.getString("metastore.type")
    val metastore =
      metastoreType match {
        case "hbase" => new HBaseMetastore
        case "delta" => new DeltaLakeMetastore
      }
    val result = metastore.getLatestCheckpoint(spark, config, project)
    result
  }
}
