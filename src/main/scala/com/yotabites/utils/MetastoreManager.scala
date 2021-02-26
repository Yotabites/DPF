package com.yotabites.utils

import java.util

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.{Failure, Success, Try}

object MetastoreManager extends LazyLogging {

  case class Metric(
                     project: String,
                     start_tm: String,
                     end_tm: String,
                     total_tm: String,
                     load_cnt: String,
                     load_tms: String,
                     prev_checkpoint: String,
                     checkpoint: String,
                     misc: String
                   )

  def result2Metric(result: Result, metricCf: String): Metric = {
    def getVal(key: String): String = Try {
      Bytes.toString(result.getValue(metricCf.getBytes, key.getBytes))
    }.getOrElse("")

    val metricNames = classOf[Metric].getDeclaredFields.map(x => x.getName).toList
    val resultMap = metricNames.map(name => (name, getVal(name))).toMap

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

  def putMetrics(metricObj: Metric, config: Config): Boolean = {
    val metricCf = config.getString("hbase.metastore.cf")
    logger.info("Updating metrics in HBase MetaStore")
    val (hTable, hConn) = HBaseUtils.getHTable(config)
    val metricNames = metricObj.getClass.getDeclaredFields.map(x => x.getName).toList
    val metricValues = metricObj.productIterator.toList.map(_.toString)
    val metricMap = metricNames.zip(metricValues)
    val revTs = Long.MaxValue - System.currentTimeMillis
    val rowKey = metricObj.project + "|" + revTs
    val put = new Put(Bytes.toBytes(rowKey))
    metricMap.foreach(x => put.addColumn(metricCf.getBytes, x._1.getBytes, x._2.getBytes))
    logger.info(s"Metrics : ${metricMap.toString}")

    val status = Try {
      hTable.put(put)
    }

    val result = status match {
      case Success(_) => logger.info(s" SuccessFully Updated Metrics to MetaStore - Row key = $rowKey")
        true
      case Failure(f) => logger.error(AppUtils.getStackTrace(f))
        logger.error("Update to HBase metastore failed...")
        false
    }
    hTable.close()
    hConn.close()
    result
  }

  def getLatestCheckpoint(config: Config, project: String): Long = {
    logger.info(s"Getting latest processed `checkpoint` value...")
    val metricCf = config.getString("hbase.metastore.cf")
    val (hTable, hConn) = HBaseUtils.getHTable(config)
    val scan = new Scan
    val prefFilter = project + "|"
    scan.setRowPrefixFilter(prefFilter.getBytes)
    val iter = hTable.getScanner(scan).iterator

    def getVal(result: Result, key: String): String = Try {
      Bytes.toString(result.getValue(metricCf.getBytes, key.getBytes))
    }.getOrElse("")

    logger.info(s"Scanning HBase table with row prefix $prefFilter ...")

    def loop(iterator: util.Iterator[Result]): Long = if (iterator.hasNext) {
      val result = iterator.next
      val end_ts = getVal(result, "checkpoint")
      end_ts.toLong
    } else 0L

    val check = loop(iter)
    val result = if (check != 0L) check else {
      logger.warn(s"end_ts value is not found in metastore defaulting to 0L ...")
      0L
    }
    logger.info(s"Checkpoint for project $project  == `$result`")
    hTable.close()
    hConn.close()
    result
  }
}
