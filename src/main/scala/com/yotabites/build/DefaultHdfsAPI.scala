package com.yotabites.build

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HDFSTransform
import org.apache.spark.sql.{DataFrame, SparkSession}

class DefaultHdfsAPI extends HDFSTransform {
  override def transform(spark: SparkSession, input: DPFConfig): DataFrame = {
    val requiredCols = input.columns
    val inpDf = spark.read.format(input.format).load(input.location).select(requiredCols.head, requiredCols.tail: _*)
    if (input.where.trim.nonEmpty) inpDf.where(input.where) else inpDf
  }

  override def save(df: DataFrame, spark: SparkSession, config: Config): Boolean = {
    df.write.format(config.getString("target.options.format"))
      .option("compression", config.getString("target.options.compression"))
      .mode(config.getString("target.options.mode"))
      .save(config.getString("target.options.location"))
    true
  }
}