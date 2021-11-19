package com.yotabites.custom

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait S3Transform {
  def transform(spark: SparkSession, config: DPFConfig, s3Config: Config): DataFrame
  def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String)
}
