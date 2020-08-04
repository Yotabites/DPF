package com.yotabites.config

import java.util

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try


object ConfigParser extends LazyLogging {

  case class DPFConfig(index: Int, id: String, source: String, location: String, join: String, format: String, 
                       where: String, columnFamily: String, transform: String, columns: List[String], custom: JSONObject)

  def parseConfig(config: Config): (List[DPFConfig], String, List[(String, Column)]) = {

    val items = config.getList("source.mapping").asScala.toList.map(x => x.unwrapped.asInstanceOf[util.HashMap[String, Any]])

    val dpf = items.map(cMap => {
      val map = cMap.asScala
      def getInt(any: Any): Int = Try { any.toString.toInt }.getOrElse(-9)
      def getString(any: Any): String = Try { any.toString }.getOrElse("")
      def getVal(key: String, map: mutable.Map[String, Any]): Any = map.getOrElse(key, null)
      def getJson(any: Any): JSONObject = Try { new JSONObject(any.toString) }.getOrElse(new JSONObject(""))

      DPFConfig.apply(
        index = getInt(getVal("index", map)),
        id = getString(getVal("id", map)),
        source = getString(getVal("source", map)),
        location = getString(getVal("location", map)),
        format = getString(getVal("format", map)),
        join = getString(getVal("join", map)),
        where = getString(getVal("where", map)),
        columnFamily = getString(getVal("column.family", map)),
        transform = getString(getVal("transform", map)),
        columns = getVal("columns", map).asInstanceOf[util.List[String]].asScala.toList,
        custom = getJson(getVal("custom", map))
      )
    })

    val where = Try { config.getString("source.where.clause") }.getOrElse(null)
    val addColsConf = Try { config.getList("source.add.columns").asScala.toList }.getOrElse(Nil)

    val addCols = addColsConf.map(x => {
      val col = x.unwrapped.asInstanceOf[util.ArrayList[String]].asScala.toList
      if (col.size != 2) logger.error("Invalid `source.add.columns` value")
      (col.head, expr(col.init.head))
    })

    (dpf, where, addCols)
  }

}
