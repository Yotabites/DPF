package com.yotabites.custom

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait HiveMngTransform {
  def transform(spark: SparkSession, input: DPFConfig): DataFrame
  def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String)
}
