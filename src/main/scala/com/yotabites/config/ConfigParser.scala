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
                       where: String, columnFamily: String, transform: String, columns: List[String], rename: Boolean,
                       unique: Boolean, options: Map[String, String], custom: JSONObject)

  def parseConfig(config: Config): (List[DPFConfig], String, List[(String, Column)]) = {

    val items = config.getList("source.mapping").asScala.toList.map(x => x.unwrapped.asInstanceOf[util.HashMap[String, Any]])

    val dpf = items.map(cMap => {
      val map = cMap.asScala
      def getVal[T](key: String, map: mutable.Map[String, Any]): T = map.getOrElse(key, null).asInstanceOf[T]
      def getJson(any: Any): JSONObject = Try { new JSONObject(any.toString) }.getOrElse(new JSONObject("{}"))

      DPFConfig.apply(
        index = getVal[Int]("index", map),
        id = getVal[String]("id", map),
        source = getVal[String]("source", map),
        location = getVal[String]("location", map),
        format = getVal[String]("format", map),
        join = getVal[String]("join", map),
        where = getVal[String]("where", map),
        columnFamily = getVal[String]("column.family", map),
        transform = getVal[String]("transform", map),
        columns = Try { getVal[util.List[String]]("columns", map).asScala.toList }.getOrElse(Nil),
        rename = Try { getVal[Boolean]("rename_cols", map) }.getOrElse(false),
        unique = Try { getVal[Boolean]("distinct", map) }.getOrElse(false),
        options = Try { map.getOrElse("options", null).asInstanceOf[util.HashMap[String, String]].asScala.toMap }.getOrElse(null),
        custom = getJson(getVal("custom", map))
      )
    })

    val where = Try { config.getString("source.where.clause") }.getOrElse(null)
    val addColsConf = Try { config.getList("source.add.columns").asScala.toList }.getOrElse(Nil)

    val addCols = addColsConf.map(x => {
      val col = x.unwrapped.asInstanceOf[util.ArrayList[String]].asScala.toList
      if (col.size != 2) logger.error("Invalid `source.add.columns` value")
      (col.head, expr(col.tail.head))
    })

    (dpf, where, addCols)
  }

}
