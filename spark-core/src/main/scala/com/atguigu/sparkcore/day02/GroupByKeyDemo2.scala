package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-24 16:17
  */
object GroupByKeyDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)))
        val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)( _.max(_),_ + _)
        println(rdd2.collect.toList)
        sc.stop()
        
    }
}
