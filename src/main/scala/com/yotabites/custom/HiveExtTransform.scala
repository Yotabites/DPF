package com.yotabites.custom

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

trait HiveExtTransform {
  def transform(spark: SparkSession, config: DPFConfig): DataFrame
}
