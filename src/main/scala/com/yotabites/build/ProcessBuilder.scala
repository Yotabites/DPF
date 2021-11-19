package com.yotabites.build

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.yotabites.config.ConfigParser
import com.yotabites.custom.{DbfsTransform, HBaseTransform, HDFSTransform, HiveExtTransform, HiveMngTransform, S3Transform}
import com.yotabites.utils.HBaseUtils._
import com.yotabites.utils.AppUtils._
import com.yotabites.utils.{MetastoreManager, Metric}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.yotabites.utils.DbfsUtils
import scala.collection.JavaConverters._
import scala.util.Try

object ProcessBuilder extends LazyLogging {
    def main(args: Array[String]): Unit = {
      if (args.length != 1) {
        println("Usage: <App Config> ")
        DbfsUtils.exit("Usage: <App Config>")
        //sys.exit(1)  do not use in databricks environment
      }

    val configFile = args(0)
    val startTime = getTime()

    /** ******************************************************
     * Parse the Config File
     * ****************************************************** */
    val config = ConfigFactory.parseFile(new File(configFile))
    val project = config.getString("source.project.name")
    val incrementalFlag = Try { config.getBoolean("source.incremental.flag") }.getOrElse(false)
    val (dpfConfigs, where, addCols) = ConfigParser.parseConfig(config)

    /** ******************************************************
     * Build Spark Context and HBase Context
     * ****************************************************** */
    val sparkConf = new SparkConf
    val sparkConfigs = config.getObject("spark-config").asScala.toList
    sparkConfigs.foreach(c => sparkConf.set(c._1, c._2.unwrapped.toString))
    sparkConf.getAll.toList.foreach(x => println(x._1 + "---" + x._2))
    val spark = SparkSession.builder.config(sparkConf).getOrCreate

    /** ******************************************************
     * Find Time Ranges
     * ****************************************************** */
    val metastoreType = config.getString("metastore.type")
    val metastore_end = System.currentTimeMillis()
    val metastore_start = if (incrementalFlag) MetastoreManager.getLatestCheckpoint(spark, config, project) else 0L
    if (metastore_start == 0) logger.warn("Seems this is an initial run for this process. Setting metastore_start as `0L`")

    var scan: Scan =
      if (metastoreType == "hbase") {
        val hbaseScan = new Scan
        hbaseScan.setTimeRange(metastore_start, metastore_end)
        hbaseScan
      } else null

    var hbaseContext : HBaseContext = if (metastoreType == "hbase") {
      new HBaseContext(spark.sparkContext, getHBaseConf(config))
    } else null

    /** ******************************************************
     * Get S3 Crentials attributes
     * ****************************************************** */

    val s3CredentialAttributes = Try {config.getConfig("s3")}.getOrElse(null)

    /** ******************************************************
     * Convert Inputs into DataFrame
     * ****************************************************** */
    dpfConfigs.foreach(input => {
      val inputDf = input.source.toLowerCase match {
        case x if x == "hdfs" => getCustomClass[HDFSTransform](input.transform, x).transform(spark, input)
        case x if x == "hive-mng" => getCustomClass[HiveMngTransform](input.transform, x).transform(spark, input)
        case x if x == "hive-ext" => getCustomClass[HiveExtTransform](input.transform, x).transform(spark, input)
        case x if x == "hbase" => {
          hbaseContext match {
            case null => hbaseContext = new HBaseContext(spark.sparkContext, getHBaseConf(config))
            case _ =>
          }
          scan match {
            case null => scan = new Scan
            case _ =>
          }
          scan.setTimeRange(metastore_start, metastore_end)
          getCustomClass[HBaseTransform](input.transform, x).transform(input, spark, hbaseContext, scan)
        }
        case x if x == "s3" => getCustomClass[S3Transform](input.transform, x).transform(spark, input, s3CredentialAttributes)
        case x if x == "dbfs" => getCustomClass[DbfsTransform](input.transform, x).transform(spark, input)
        case _ => logger.error("Gotta upgrade to premium for " + input.source + " input"); sys.exit(99); null
      }
      val df = if (input.rename) inputDf.toDF(inputDf.columns.map(x => input.id + "_" + x): _*) else inputDf
      df.createOrReplaceTempView(input.id)
      logger.info(s"Created temp view - ${input.id}")
    })

    /** ******************************************************
     * Apply Join conditions, build output DataFrame
     * ****************************************************** */
    val joinSQL = dpfConfigs.sortBy(_.index).map(x => x.join).mkString(" ")
    val whereClause = if (null != where) where else "1 = 1"
    val joinDf = spark.sql(s"select * from $joinSQL").where(whereClause)
    val outDf = addCols.foldLeft(joinDf) { case (tmpDf, col) => tmpDf.withColumn(col._1, col._2) }

    /** ******************************************************
     * Write output DataFrame to Target
     * ****************************************************** */
    logger.info(">>>>> target.place: " + config.getString("target.place"))
    val customClass = Try { config.getString("target.class") }.getOrElse("")
    val (targetCt, misc) = config.getString("target.place") match {
      case "hbase" => getCustomClass[HBaseTransform](customClass, "hbase").save(outDf, spark, hbaseContext, config)
      case "hdfs" => getCustomClass[HDFSTransform](customClass, "hdfs").save(outDf, spark, config)
      case "hive" => getCustomClass[HiveMngTransform](customClass, "hive").save(outDf, spark, config)
      case "s3" => getCustomClass[S3Transform](customClass, "s3").save(outDf, spark, config)
      case "dbfs" => getCustomClass[DbfsTransform](customClass, "dbfs").save(outDf, spark, config)
      case x => logger.error("Gotta upgrade to premium for " + x + " target"); sys.exit(99); (-1L, "")
    }

    /** ******************************************************
     * Write metrics to CMF Meta store
     * ****************************************************** */
    val endTime = getTime()
    val metricObj = Metric.apply(
      project = project,
      start_tm = startTime,
      end_tm = endTime,
      total_tm = diffTime(startTime, endTime).toString,
      load_cnt = targetCt.toString,
      load_tms = getTime(),
      prev_checkpoint = metastore_start.toString,
      checkpoint = metastore_end.toString,
      misc = misc
    )
    val res = MetastoreManager.putMetrics(spark, metricObj, config)

    //spark.stop()

    if (res) {
      logger.info("Process finished successfully...")
      //sys.exit(0)  do not use in databricks environment
    }
    else {
      logger.error("Process failed.")
      DbfsUtils.exit("Process failed.")
      //sys.exit(-1)  do not use in databricks environment
    }
  }
}
