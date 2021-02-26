package com.yotabites.build

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HDFSTransform
import com.yotabites.utils.AppUtils.writeTargetDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

class DefaultHdfsAPI extends HDFSTransform {
  override def transform(spark: SparkSession, input: DPFConfig): DataFrame = {
    val requiredCols = input.columns
    val opts: Map[String, String] = if(input.options == null) Map() else input.options
    val df = spark.read.options(opts).format(input.format).load(input.location)
    val inpDf = if (requiredCols.isEmpty) df else df.selectExpr(requiredCols: _*)
    val tDf = if (input.where != null && input.where.trim.nonEmpty) inpDf.where(input.where) else inpDf
    if(input.unique) tDf.distinct else tDf
  }

  override def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String) = {
    (writeTargetDataFrame(spark, df, config), "")
  }
}