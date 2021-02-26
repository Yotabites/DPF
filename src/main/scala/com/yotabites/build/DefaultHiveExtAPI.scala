package com.yotabites.build

import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HiveExtTransform
import org.apache.spark.sql.{DataFrame, SparkSession}

class DefaultHiveExtAPI extends HiveExtTransform {
  override def transform(spark: SparkSession, input: DPFConfig): DataFrame = {
    val requiredCols = input.columns

    val inpDf = spark.sql(s"select * from ${input.location}").selectExpr(requiredCols: _*)
    val tDf = if (input.where.trim.nonEmpty) inpDf.where(input.where) else inpDf
    if(input.unique) tDf.distinct else tDf
  }
}
