package com.yotabites.build

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HBaseTransform
import com.yotabites.utils.HBaseBulkLoad
import com.yotabites.utils.HBaseUtils._
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

class DefaultHBaseAPI extends HBaseTransform {
  override def transform(input: DPFConfig, spark: SparkSession, hbaseCtx: HBaseContext, scan: Scan): DataFrame = {
    val tableName = input.location
    val cf = input.columnFamily
    val mappingJson = input.custom.getJSONObject("mapping")

    val inputCols = mappingJson.keys.asScala.toList
    val kvPairs = inputCols.map(key => mappingJson.getString(key))

    val hbaseRdd = hbaseCtx.hbaseRDD(TableName.valueOf(tableName), scan)
    val rowRdd = hbaseRdd.map(result => {
      val rowKey = Bytes.toString(result._1.copyBytes)
      val values = kvPairs.map(x => getValFromRes(result._2, cf, x))
      Row.fromSeq(rowKey :: values)
    })

    val schema = StructType(("rowkey" :: inputCols).map(x => StructField(x, StringType)))
    spark.createDataFrame(rowRdd, schema)
  }

  override def save(df: DataFrame, spark: SparkSession, hbaseCtx: HBaseContext, config: Config): Boolean = {
    val cols = df.columns
    val cf = config.getString("target.options.cf")
    val tbl = config.getString("target.options.tablename")
    val isBulkLoad = Try { config.getBoolean("target.options.bulkload") }.getOrElse(false)

    def getUUID: String = java.util.UUID.randomUUID.toString
    def row2put(row: Row): Put = {
      val put = new Put(getUUID.getBytes)
      val map = row.getValuesMap[Any](cols)
      map.foreach(x => put.addColumn(cf.getBytes, x._1.getBytes, convertToBytes(x._2)))
      put
    }

    def row2KV(row: Row): (String, List[KeyValue]) = {
      val rk = getUUID
      val map = row.getValuesMap[Any](cols)
      (rk, map.toList.map(x => new KeyValue(rk.getBytes, cf.getBytes, x._1.getBytes, convertToBytes(x._2))))
    }

    /**
     * Do BulkLoad or BulkPut
     * */
    if (isBulkLoad) {
      val numRegions = config.getInt("target.options.num-regions")
      val stgDir = config.getString("target.options.bulk-stg")
      val kvRdd = df.rdd.map(row2KV)
      HBaseBulkLoad.doBulkLoad(config, kvRdd, tbl, cf, numRegions, stgDir)
    } else {
      val putRdd = df.rdd.map(row2put)
      hbaseCtx.bulkPut[Put](putRdd, TableName.valueOf(tbl), a => a)
    }

    true
  }

  /**
   * Functions to convert to/from Bytes
   * */

  def convertToBytes(obj: Any): Array[Byte] = Try {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(obj)
      bos.toByteArray
    } catch {
      case t: Throwable => throw t
    } finally {
      if (bos != null) bos.close()
      if (out != null) out.close()
    }
  }.getOrElse(Array())

  def convertToObject(byteArr: Array[Byte]): Any = Try {
    try {
      val bis = new ByteArrayInputStream(byteArr)
      val in = new ObjectInputStream(bis)
      try {
        return in.readObject()
      } catch {
        case t: Throwable => throw t
      } finally {
        if (bis != null) bis.close()
        if (in != null) in.close()
      }
    }
  }.getOrElse(null)
}
