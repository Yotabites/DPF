package com.yotabites.custom

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.{DataFrame, SparkSession}

trait HBaseTransform {
  def transform(config: DPFConfig, spark: SparkSession, hbaseCtx: HBaseContext, scan: Scan): DataFrame
  def save(df: DataFrame, spark: SparkSession, hbaseCtx: HBaseContext, config: Config): (Long, String)
}
