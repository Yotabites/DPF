package com.yotabites.build

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HiveMngTransform
import org.apache.spark.sql.{DataFrame, SparkSession}

class DefaultHiveMngAPI extends HiveMngTransform {
  override def transform(spark: SparkSession, input: DPFConfig): DataFrame = {
    val databaseName = input.location.split(".")(0)
    val tableName = input.location.split(".")(1)
    val requiredCols = input.columns

    val hive = HiveWarehouseBuilder.session(spark).build()
    hive.setDatabase(databaseName)
    val inpDf = hive.table(tableName).select(requiredCols.head, requiredCols.tail: _*)
    if (input.where.trim.nonEmpty) inpDf.where(input.where) else inpDf
  }

  override def save(df: DataFrame, spark: SparkSession, config: Config): Boolean = {
    val tableName = config.getString("target.options.table")

    df.write
      .format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
      .option("table", tableName)
      .save
      true
  }
}
