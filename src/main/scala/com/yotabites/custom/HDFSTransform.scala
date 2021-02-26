package com.yotabites.custom

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait HDFSTransform {
  def transform(spark: SparkSession, config: DPFConfig): DataFrame
  def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String)
}
