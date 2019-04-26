package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 09:32
  */
object FoldByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(("a", 1), ("b", 3), ("a", 4)))
        val resultRDD: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)
        println(resultRDD.collect.toList)
        sc.stop()
        
    }
}
