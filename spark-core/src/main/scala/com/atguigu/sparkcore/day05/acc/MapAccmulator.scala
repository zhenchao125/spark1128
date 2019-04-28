package com.atguigu.sparkcore.day05.acc

import org.apache.spark.util.AccumulatorV2

class MapAccmulator extends AccumulatorV2[Int, Map[String, Double]] {
    var map = Map[String, Double]()
    
    override def isZero: Boolean = map.isEmpty
    
    
    override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
        val acc = new MapAccmulator
        acc.map ++= map
        acc
    }
    
    override def reset(): Unit = map = Map[String, Double]()
    
    
    override def add(v: Int): Unit = {
        map += "sum" -> (map.getOrElse("sum", 0d) + v)
        map += "count" -> (map.getOrElse("count", 0d) + 1)
    }
    
    override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = other match {
        case o: MapAccmulator =>
            map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
            map += "count" -> (map.getOrElse("count", 0d) + o.map.getOrElse("count", 0d))
        case _ =>
    }
    
    
    override def value: Map[String, Double] = {
        map += "avg" -> map.getOrElse("sum", 0d) / map.getOrElse("count", 1d)
        map
    }
}

/*
value :  (sum, count, avg)

value: Map("sum" -> sum, ...)
 */
