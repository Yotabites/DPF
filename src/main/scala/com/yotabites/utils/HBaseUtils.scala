package com.yotabites.utils

import java.text.SimpleDateFormat

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object HBaseUtils extends LazyLogging {

  def getHBaseConf(config: Config): Configuration = {
    val conf = HBaseConfiguration.create
    config.getStringList("hbase.source.files").asScala.toList.foreach(file => conf.addResource(new Path(file)))
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
    conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName)
    conf
  }

  def getHTable(config: Config): (Table, Connection) = {
    logger.info("Creating HBase connection ")
    val conf = getHBaseConf(config)
    val connection = ConnectionFactory.createConnection(conf)
    val con = Try {
      connection.getTable(TableName.valueOf(config.getString("hbase.metastore.table")))
    }
    val hTable = con match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage)
        logger.error(f.getStackTrace.toString)
        logger.error("Can't connect to HBase")
        null
    }

    (hTable, connection)
  }

  def getCellName(cell: Cell): String = Bytes.toString(CellUtil.cloneQualifier(cell))

  def getCellValue(cell: Cell): String = Bytes.toString(CellUtil.cloneValue(cell))

  def getValues(inputList: List[(String, String)], endList: List[String]): List[String] =
    endList.map(x => inputList.filter(_._1.endsWith(x)).lastOption.map(_._2).orNull)

  def getValFromRes(result: Result, cf: String, cellName: String): String =
    Try { Bytes.toString(result.getValue(cf.getBytes, cellName.getBytes)) }.getOrElse(null)

  def getSchema(list: List[String]): StructType = StructType(list.map(x => StructField(x, StringType)))

  def getTime(pattern: String = "yyyy-MM-dd HH:mm:ss"): String = new SimpleDateFormat(pattern).format(System.currentTimeMillis)

  def diffTime(start: String, end: String): Long = {
    val a = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start).getTime
    val b = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(end).getTime
    (b - a) / 1000
  }

  def parseLong(str: String): Long = Try {str.toLong}.getOrElse(-99)

  def getBytes(str: String): Array[Byte] = Try {str.getBytes}.getOrElse(Array())
}