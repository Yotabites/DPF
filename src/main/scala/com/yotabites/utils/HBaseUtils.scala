package com.yotabites.utils

import java.text.SimpleDateFormat

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try
import scala.collection.JavaConverters._

object HBaseUtils {

  def getHBaseConf(config: Config): Configuration = {
    val conf = HBaseConfiguration.create
    config.getStringList("hbase.source.files").asScala.toList.foreach(file => conf.addResource(new Path(file)))
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
    conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName)
    conf
  }

  def getCellName(cell: Cell): String = Bytes.toString(CellUtil.cloneQualifier(cell))

  def getCellValue(cell: Cell): String = Bytes.toString(CellUtil.cloneValue(cell))

  def getValues(inputList: List[(String, String)], endList: List[String]): List[String] =
    endList.map(x => inputList.filter(_._1.endsWith(x)).lastOption.map(_._2).orNull)

  def getValFromRes(result: Result, cf: String, cellName: String): String =
    Try { Bytes.toString(result.getValue(cf.getBytes, cellName.getBytes)) }.getOrElse(null)

  def getSchema(list: List[String]): StructType = StructType(list.map(x => StructField(x, StringType)))

  def getTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis)

  def diffTime(start: String, end: String): Long = {
    val a = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start).getTime
    val b = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(end).getTime
    (b - a) / 1000
  }

  def parseLong(str: String): Long = Try {str.toLong}.getOrElse(-99)

  def getBytes(str: String): Array[Byte] = Try {str.getBytes}.getOrElse(Array())
}