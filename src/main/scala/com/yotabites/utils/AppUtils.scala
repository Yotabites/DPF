package com.yotabites.utils

import com.typesafe.scalalogging.LazyLogging
import com.yotabites.build.{DefaultHBaseAPI, DefaultHdfsAPI, DefaultHiveExtAPI, DefaultHiveMngAPI}

import scala.util.Try

object AppUtils extends LazyLogging {

  def getCustomClass[T](clazz: String, which: String): T = {
    val (customClass, defaultObj) = which.toLowerCase match {
      case "hdfs" => (clazz, new DefaultHdfsAPI)
      case "hive-ext" => (clazz, new DefaultHiveExtAPI)
      case "hive-mng" => (clazz, new DefaultHiveMngAPI)
      case "hive" => (clazz, new DefaultHiveMngAPI)
      case "hbase" => (clazz, new DefaultHBaseAPI)
    }

    val transformationModel = if (customClass.isEmpty) {
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
}
