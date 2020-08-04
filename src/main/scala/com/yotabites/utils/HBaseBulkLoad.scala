package com.yotabites.utils

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.spark.BulkLoadPartitioner
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object HBaseBulkLoad {

  case class HBaseKeyValue(rowKey: String, kv: KeyValue)

  def doBulkLoad(
                  config: Config,
                  kvPairsRdd: RDD[(String, List[KeyValue])],
                  hbaseTable: String,
                  cf: String,
                  numRegions: Int,
                  stgDir: String
                ): Unit = {

    /** ************************************************
     * Build Split Keys - for HBase table creation
     * Sort the input dataset based on RowKey
     * Split it into 'numRegions' partitions
     * Take the first key from each partition
     * *************************************************/
    val hConf = HBaseUtils.getHBaseConf(config)

    val splitKeys = if (tableExists(hbaseTable, hConf)) {
      getHBaseKeys(hbaseTable, hConf)
    } else {
      val splitKeySize = numRegions - 1
      val sizeOfRegion: Double = kvPairsRdd.count / splitKeySize.toDouble
      val indices = Stream.iterate(0D)(x => x + sizeOfRegion).map(math.round).take(splitKeySize).toList
      kvPairsRdd.map(_._1).sortBy(x => x).zipWithIndex.filter(x => indices.contains(x._2))
        .map(_._1).filter(_ != null).collect.distinct.map(_.getBytes)
    }
    /** ************************************************
     * Transform dataset to HFile format
     * Sort rows and cells in each row in lexicographical order
     * *************************************************/
    val kvs = kvPairsRdd.flatMap(x => preparePut(x, cf)).map(a => (a.rowKey, a.kv))
      .repartitionAndSortWithinPartitions(new BulkLoadPartitioner(splitKeys))

    /** ************************************************
     * Save HFiles in HBase staging directory
     * And Create HBase table; load the HFiles into it
     * *************************************************/
    saveAsHFile(kvs, hConf, stgDir)
    createAndLoadTable(hbaseTable, cf, hConf, splitKeys, stgDir)
  }

  /**
   * convertToBytes is used to convert objects to byteArray
   * since deserialization will be properly convert them back to objects
   * HBase `Bytes` API does not deserialize well.
   **/
  def preparePut(kvList: (String, List[KeyValue]), cf: String): List[HBaseKeyValue] = {
    val (rowKey, kvMap) = kvList
    val allKvPairs = kvMap.map(x => HBaseKeyValue(rowKey + "$" + cf + Bytes.toString(CellUtil.cloneQualifier(x)), x))
    allKvPairs
  }

  def saveAsHFile(putRDD: RDD[(String, KeyValue)], hConf: Configuration, outputPath: String): Unit = {
    putRDD.map(x => (new ImmutableBytesWritable(x._1.split("\\$")(0).getBytes), x._2))
      .saveAsNewAPIHadoopFile(outputPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2], hConf)

    // prepare HFiles for incremental load
    // set folders permissions read/write/exec for all
    def setRecursivePermission(path: Path): Unit = {
      val fs = FileSystem.get(hConf)
      val rwx = new FsPermission("777")
      fs.setPermission(path, rwx)
      val listFiles = fs.listStatus(path)
      listFiles.foreach { f =>
        val p = f.getPath
        fs.setPermission(p, rwx)
        if (f.isDirectory) setRecursivePermission(p)
      }
    }

    setRecursivePermission(new Path(outputPath))
  }

  def tableExists(tableName: String, hConf: Configuration): Boolean = {
    val conn = ConnectionFactory.createConnection(hConf)
    val hAdmin = conn.getAdmin
    hAdmin.tableExists(TableName.valueOf(tableName))
  }

  def getHBaseKeys(tableName: String, hConf: Configuration): Array[Array[Byte]] = {
    val conn = ConnectionFactory.createConnection(hConf)
    val hAdmin = conn.getAdmin
    val regions = hAdmin.getRegions(TableName.valueOf(tableName)).asScala.toArray
    regions.map(x => x.getEndKey).filter(_.nonEmpty)
  }

  def createAndLoadTable(tableName: String, columnFamily: String, hConf: Configuration, splitKeys: Array[Array[Byte]],
                         outPath: String): Unit = {
    println("TEST: COMPRESSION ------" + hConf.get("hfile.compression") + "...")
    val conn = ConnectionFactory.createConnection(hConf)
    val hAdmin = conn.getAdmin
    val lih = new LoadIncrementalHFiles(hConf)
    val regionLocator = new HRegionLocator(TableName.valueOf(tableName), conn.asInstanceOf[ClusterConnection])

    if (!tableExists(tableName, hConf)) {
      val hTable = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      hTable.
        setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes).
          setCompressionType(Compression.Algorithm.SNAPPY).build)
      hAdmin.createTable(hTable.build(), splitKeys)
      println(s"*** Table $tableName is created. Starting Bulk Load : " + tableName)
    }

    lih.doBulkLoad(new Path(outPath), hAdmin, conn.getTable(TableName.valueOf(tableName)), regionLocator)
  }
}
