package com.yotabites.utils

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.yotabites.build.{DefaultHBaseAPI, DefaultHdfsAPI, DefaultHiveExtAPI, DefaultHiveMngAPI, DefaultS3API, DefaultDbfsAPI}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object AppUtils extends LazyLogging {

  def getCustomClass[T](clazz: String, which: String): T = {
    val (customClass, defaultObj) = which.toLowerCase match {
      case "hdfs" => (clazz, new DefaultHdfsAPI)
      case "hive-ext" => (clazz, new DefaultHiveExtAPI)
      case "hive-mng" => (clazz, new DefaultHiveMngAPI)
      case "hive" => (clazz, new DefaultHiveMngAPI)
      case "hbase" => (clazz, new DefaultHBaseAPI)
      case "s3" =>   (clazz, new DefaultS3API)
      case "dbfs" =>   (clazz, new DefaultDbfsAPI)
    }

    val transformationModel = if (null == customClass || customClass.isEmpty) {
      logger.info(s"`$which.transform` not specified. Using ${defaultObj.getClass.getSimpleName}...")
      defaultObj
    } else {
      logger.info(s" `$which.transform` - Transformation class : $customClass")
      Try {
        Class.forName(customClass).newInstance.asInstanceOf[T]
      }.getOrElse({
        logger.error(s"$customClass is not available in the classpath or not a sub-class of its Trait. Exiting...")
        sys.exit(-8)
        null
      })
    }
    transformationModel.asInstanceOf[T]
  }

  def writeTargetDataFrame(spark: SparkSession, df: DataFrame, config: Config,
                           location: String = null, doNotCount: Boolean = false): Long = {
    val path = if (null == location) config.getString("target.options.location") else location
    df.write.format(config.getString("target.options.format"))
      .option("compression", config.getString("target.options.compression"))
      .mode(config.getString("target.options.mode"))
      .save(path)

    if(doNotCount) 0L else spark.read.format(config.getString("target.options.format")).load(path).count
  }

  def getStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter
    throwable.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}
