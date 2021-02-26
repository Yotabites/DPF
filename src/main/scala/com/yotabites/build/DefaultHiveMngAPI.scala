package com.yotabites.build

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
import com.typesafe.config.Config
import com.yotabites.config.ConfigParser.DPFConfig
import com.yotabites.custom.HiveMngTransform
import org.apache.spark.sql.{DataFrame, SparkSession}

class DefaultHiveMngAPI extends HiveMngTransform {
  override def transform(spark: SparkSession, input: DPFConfig): DataFrame = {
    val databaseName = input.location.split("\\.")(0)
    val tableName = input.location.split("\\.")(1)
    val requiredCols = input.columns
    val where = if (input.where != null && input.where.trim.nonEmpty) input.where else "1 = 1"

    val hive = HiveWarehouseBuilder.session(spark).build()
    hive.setDatabase(databaseName)
    val df = hive.table(tableName)
    val inpDf = if (requiredCols.isEmpty) df else df.selectExpr(requiredCols: _*)
    val tDf = inpDf.where(where)
    if(input.unique) tDf.distinct else tDf
  }

  override def save(df: DataFrame, spark: SparkSession, config: Config): (Long, String) = {
    val tableName = config.getString("target.options.table")

    df.write
      .format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
      .option("table", tableName)
      .save

    (-1L, "")
  }
}
