package com.yotabites.build

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.yotabites.config.ConfigParser
import com.yotabites.custom.{HBaseTransform, HDFSTransform, HiveExtTransform, HiveMngTransform}
import com.yotabites.utils.HBaseUtils._
import com.yotabites.utils.AppUtils._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

object ProcessBuilder extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: <App Config> ")
      sys.exit(1)
    }

    val configFile = args(0)
    val startTime = getTime

    /** ******************************************************
     * Parse the Config File
     * ****************************************************** */
    val config = ConfigFactory.parseFile(new File(configFile))
    val (dpfConfigs, where, addCols) = ConfigParser.parseConfig(config)

    /** ******************************************************
     * Find Time Ranges for HBase Scan
     * TODO: Implement incremental logic using CMF meta store
     * ****************************************************** */
    val scan = new Scan

    /** ******************************************************
     * Build Spark Context and HBase Context
     * ****************************************************** */
    val sparkConf = new SparkConf
    val sparkConfigs = config.getObject("spark-config").asScala.toList
    sparkConfigs.foreach(c => sparkConf.set(c._1, c._2.unwrapped.toString))
    sparkConf.getAll.toList.foreach(x => println(x._1 + "---" + x._2))
    val spark = SparkSession.builder.config(sparkConf).getOrCreate
    val hbaseContext = new HBaseContext(spark.sparkContext, getHBaseConf(config))

    /** ******************************************************
     * Convert Inputs into DataFrame
     * ****************************************************** */
    dpfConfigs.foreach(input => {
      val inputDf = input.source.toLowerCase match {
        case x if x == "hdfs" => getCustomClass[HDFSTransform](input.transform, x).transform(spark, input)
        case x if x == "mng_hive" => getCustomClass[HiveMngTransform](input.transform, x).transform(spark, input)
        case x if x == "ext_hive" => getCustomClass[HiveExtTransform](input.transform, x).transform(spark, input)
        case x if x == "hbase" => getCustomClass[HBaseTransform](input.transform, x).transform(input, spark, hbaseContext, scan)
        case _ => logger.error("Gotta upgrade to premium for " + input.source + " input"); sys.exit(99); null
      }
      inputDf.createOrReplaceTempView(input.id)
      logger.info(s"Created temp view - ${input.id}")
    })

    /** ******************************************************
     * Apply Join conditions, build output DataFrame
     * ****************************************************** */
    val joinSQL = dpfConfigs.sortBy(_.index).map(x => x.join).mkString(" ")
    val joinDf = spark.sql(s"select * from $joinSQL").where(where)
    val outDf = addCols.foldLeft(joinDf) { case (tmpDf, col) => tmpDf.withColumn(col._1, col._2) }

    /** ******************************************************
     * Write output DataFrame to Target
     * ****************************************************** */
    val customClass = Try { config.getString("target.class") }.getOrElse("default")
    val res = config.getString("target.place") match {
      case "hbase" => getCustomClass[HBaseTransform](customClass, "hbase").save(outDf, spark, hbaseContext, config)
      case "hdfs" => getCustomClass[HDFSTransform](customClass, "hdfs").save(outDf, spark, config)
      case "hive" => getCustomClass[HiveMngTransform](customClass, "hive").save(outDf, spark, config)
      case x => logger.error("Gotta upgrade to premium for " + x + " target"); sys.exit(99); false
    }

    /** ******************************************************
     * TODO: Write metrics to CMF Meta store
     * ****************************************************** */


    spark.stop()

    if (res) {
      logger.info("Process finished successfully...")
      sys.exit(-1)
    }
    else {
      logger.error("Process failed.")
      sys.exit(0)
    }
  }
}
