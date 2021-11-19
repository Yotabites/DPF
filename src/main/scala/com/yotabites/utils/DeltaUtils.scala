package com.yotabites.utils

import com.typesafe.config.Config
import com.yotabites.utils.AppUtils.writeTargetDataFrame
import io.delta.tables.DeltaTable
import com.typesafe.scalalogging.LazyLogging
import com.yotabites.config.ConfigParser.DPFConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.util.Try

object DeltaUtils extends LazyLogging {

  def deltaRead(spark: SparkSession, input: DPFConfig, readLocation: String = null): DataFrame = {
    val stream = input.stream
    val opts: Map[String, String] = if(input.options == null) Map() else input.options
    val path = if (null == readLocation)  input.location else readLocation
    logger.info(s">>>>> deltaRead location: $path")
    val df = if (!stream) {
      spark.read.options(opts).format(input.format).load(path)
    } else {
      spark.readStream.options(opts).format(input.format).load(path)
    }
    df
  }

  def deltaWrite(spark: SparkSession, df: DataFrame, config: Config,
                 writeLocation: String = null, doNotCount: Boolean = false): Long = {
    logger.info(">>>>> Inside deltaWrite.")
    val mode = config.getString("target.options.mode")
    val path = if (null == writeLocation) config.getString("target.options.location") else writeLocation
    logger.info(s">>>>> target.options.mode: $mode")
    logger.info(s">>>>> deltaWrite location: $path")
    mode match {
      case "overwrite" | "append" =>
        val stream = Try {config.getBoolean("target.options.stream")}.getOrElse(false)
        logger.info(s">>>>> target.options.stream: $stream")
        val count = if (!stream) {
          // write to a file
          writeTargetDataFrame(spark, df, config, writeLocation, doNotCount)
        } else {
          // write to a stream
          val streamingQuery = writeStream(df, path, config)
          logger.info(">>>>> successfully started streaming.")
          0L
        }
        count
      case "upsert" =>
        val joinCondition = config.getString("target.options.delta.upsert.condition")
        upsert(df, path, joinCondition)
        if(doNotCount) 0L else spark.read.format(config.getString("target.options.format")).load(path).count
      case "merge" =>
        val joinCondition = config.getString("target.options.delta.merge.condition")
        val setStatement = config.getString("target.options.delta.merge.set")
        merge(df, path, joinCondition, setStatement)
        if(doNotCount) 0L else spark.read.format(config.getString("target.options.format")).load(path).count
      case "logicalDelete" =>
        val joinCondition = config.getString("target.options.delta.logicalDelete.condition")
        val setStatement = config.getString("target.options.delta.logicalDelete.set")
        logicalDelete(df, path, joinCondition, setStatement)
        if(doNotCount) 0L else spark.read.format(config.getString("target.options.format")).load(path).count
      case _ =>
        logger.error(s"Not a valid Delta Lake write mode- $mode")
        0L
    }
  }

  def writeStream(df: DataFrame, location: String, config: Config): StreamingQuery = {
    val format = Try{config.getString("target.options.format")}.getOrElse("delta")
    val mode = Try{config.getString("target.options.mode")}.getOrElse("append")
    val path = if (null == location) config.getString("target.options.location") else location
    val checkpointLocation = {
      val cp = Try{config.getString("target.options.streamCheckpointLocation")}.getOrElse(path + "_checkpoint")
      if (cp.isEmpty) path + "_checkpoint"
      else cp
    }
    logger.info(s">>>>> Stream checkpoint location- $checkpointLocation")
    logger.info(s">>>>> Stream output location- $path")
    df.writeStream
      .format(format)
      .outputMode(mode)
      .option("checkpointLocation", checkpointLocation)
      .start(path)

  }

  /**
   *
   * when match found - update row, else when no match - insert row
   *
   */
  def upsert(sourceDf: DataFrame, targetLocation: String, joinCondition: String): Unit = {
    val targetDeltaTable = DeltaTable.forPath(targetLocation)
    targetDeltaTable.as("target")
      .merge(sourceDf.as("source"),joinCondition)
      .whenMatched.updateAll
      .whenNotMatched.insertAll
      .execute
  }

  /**
   *
   * when match found - do update using target.options.delta.merge.set
   *
   */
  def merge(sourceDf: DataFrame, targetLocation: String, joinCondition: String, setStatement: String): Unit = {
    val targetDeltaTable = DeltaTable.forPath(targetLocation)
    val updateMap = getMap(setStatement)
    targetDeltaTable.as("target")
      .merge(sourceDf.as("source"),joinCondition)
      .whenMatched.updateExpr(updateMap)
      .execute
  }

  /**
   *
   * when match found - do update using target.options.delta.logicalDelete.set
   *
   */
  def logicalDelete(sourceDf: DataFrame, targetLocation: String, joinCondition: String, setStatement: String): Unit = {
    val targetDeltaTable = DeltaTable.forPath(targetLocation)
    val updateMap = getMap(setStatement)
    targetDeltaTable.as("target")
      .merge(sourceDf.as("source"),joinCondition)
      .whenMatched.updateExpr(updateMap)
      .execute
  }

  /**
   * converts set statement to a Map
   *
   * @param setStatement: a string in the format - "target.column1 = source.column1 and target.column2 = 'N'"
   * @return Map: a Map object containing - Map("target.column1" -> "source.column1", "target.column2" -> "'N'")
   */
  private def getMap(setStatement: String): Map[String, String] = {
    val retVal =
      setStatement.split(" and ").map(ele => ((ele.split("=")(0).trim) -> (ele.split("=")(1).trim))).toMap
    logger.info(s">>>>> $setStatement converted to: " + retVal.toString())
    retVal
  }

}
