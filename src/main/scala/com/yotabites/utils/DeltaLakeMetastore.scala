package com.yotabites.utils

//import java.util

import DbfsUtils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
//import io.delta.tables._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lit, map}
import org.json.JSONObject

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class DeltaLakeMetastore extends Metastore with LazyLogging {

  final val s3UrlPattern : Regex = "^[sS]3.?://.*$".r
  final val dbfsUrlPattern: Regex = "^/FileStore/.*$".r

  def result2Metric(any: Any, metricCf: String): Metric = {
    val row = any.asInstanceOf[Row]

    val resultMap = row.getValuesMap[String](row.schema.fieldNames)

    Metric.apply(
      project = resultMap.getOrElse("project", ""),
      start_tm = resultMap.getOrElse("start_tm", ""),
      end_tm = resultMap.getOrElse("end_tm", ""),
      total_tm = resultMap.getOrElse("total_tm", ""),
      load_cnt = resultMap.getOrElse("load_cnt", ""),
      load_tms = resultMap.getOrElse("load_tms", ""),
      prev_checkpoint = resultMap.getOrElse("prev_checkpoint", ""),
      checkpoint = resultMap.getOrElse("checkpoint", ""),
      misc = resultMap.getOrElse("misc", "")
    )
  }

  def putMetrics(spark: SparkSession, metricObj: Metric, config: Config): Boolean = {
    val metricTable = config.getString("metastore.table")
    val customJson =  new JSONObject(Try {config.getString("metastore.custom")}.getOrElse("{}"))
    val metricTableWriteLocation = metricTable match  {
      case s3UrlPattern() | dbfsUrlPattern() => metricTable
      case _ =>  setupMountPoint(metricTable, config.getConfig("s3"), customJson)
    }
    logger.info(s">>>>> Updating metrics in DeltaLake MetaStore $metricTableWriteLocation")
    val metricNames = metricObj.getClass.getDeclaredFields.map(x => x.getName).toList
    val metricValues = metricObj.productIterator.toList.map(_.toString)
    val metricMap = metricNames.zip(metricValues).toMap
    logger.info(s">>>>> Metrics : ${metricMap.toString}")

    import spark.implicits._
    val matricdf = metricMap.tail
      .foldLeft(Seq(metricMap.head._2).toDF(metricMap.head._1))((acc,curr) => acc.withColumn(curr._1,lit(curr._2)))
    matricdf.show(false)

    val status = Try {
      matricdf.write.mode("append").format("delta").save(metricTableWriteLocation)
    }

    val result = status match {
      case Success(_) => logger.info(s" SuccessFully Updated Metrics to MetaStore to DeltaLake")
        true
      case Failure(f) => logger.error(AppUtils.getStackTrace(f))
        logger.error("Update to DeltaLake metastore failed...")
        false
    }

    result
  }

  def getLatestCheckpoint(spark: SparkSession, config: Config, project: String): Long = {
    logger.info(s"Getting latest processed `checkpoint` value...")
    val metricTable = config.getString("metastore.table")
    val customJson =  new JSONObject(Try {config.getString("metastore.custom")}.getOrElse("{}"))
    val metricTableReadLocation = metricTable match  {
      case s3UrlPattern() | dbfsUrlPattern() => metricTable
      case _ =>  setupMountPoint(metricTable, config.getConfig("s3"), customJson)
    }
    val metricDf = spark.read.format("delta").load(metricTableReadLocation)
    metricDf.createOrReplaceTempView("_ms")
    val metricDfSorted = spark.sql(s"select * from _ms where project = '$project' order by start_tm desc")
    val checkPoint = Try {
      val cp = metricDfSorted.first().getAs("checkpoint").asInstanceOf[String]
      if (cp.isEmpty) {
        logger.warn(s"checkpoint value is not found in DeltaLake metastore table '$metricTable', defaulting to 0L ...")
        0L
      } else cp.toLong
    }

    val result : Long = checkPoint match {
      case Success(cp) => logger.info(s" SuccessFully Obtained Checkpoint '$cp' from DeltaLake metastore table '$metricTable'")
        cp
      case Failure(f) => logger.error(AppUtils.getStackTrace(f))
        logger.error("Error reading checkpoint from DeltaLake metastore table '$metricTable' ...")
        -1L
    }

    logger.info(s"Checkpoint for project $project  == `$result`")
    result
  }
}
