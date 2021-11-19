package com.yotabites.build

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.S3Transform
import com.yotabites.utils.AppUtils.writeTargetDataFrame
import com.yotabites.utils.DbfsUtils._
import com.yotabites.utils.DeltaUtils.deltaWrite
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.LazyLogging
import org.json.JSONObject
import com.yotabites.utils.DeltaUtils.deltaRead

import scala.collection.mutable
import scala.util.Try

class DefaultS3API extends S3Transform with LazyLogging {
  override def transform(spark: SparkSession, input: DPFConfig, s3CredentialAttributes: Config): DataFrame = {
    val requiredCols = input.columns
    setupMountPoint(input.location, s3CredentialAttributes, input.custom)
    val readLocation = s3MountPrefix + input.location
    val df = if (input.format == "delta") {
      deltaRead(spark, input, readLocation)
    } else {
      val opts: Map[String, String] = if(input.options == null) Map() else input.options
      spark.read.options(opts).format(input.format).load(readLocation)
    }
    val inpDf = if (requiredCols.isEmpty) df else df.selectExpr(requiredCols: _*)
    val tDf = if (input.where != null && input.where.trim.nonEmpty) inpDf.where(input.where) else inpDf
    if(input.unique) tDf.distinct else tDf
  }

  override def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String) = {
    val customJson = new JSONObject(Try {
      config.getString("custom")
    }.getOrElse("{}"))
    val location = config.getString("target.options.location")
    setupMountPoint(location, config.getConfig("s3"), customJson)
    val writeLocation = s3MountPrefix + location
    val format = config.getString("target.options.format")
    logger.info(s">>>>> target.options.format: $format")
    if (format == "delta") {
      (deltaWrite(spark, df, config, writeLocation), writeLocation)
    } else {
      (writeTargetDataFrame(spark, df, config, writeLocation), writeLocation)
    }
  }


}