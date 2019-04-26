package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 10:55
  */
object MapValues {
    def main(args: Array[String]): Unit = {
        val arr = Array(("a", 1), ("b", 10), ("b", 20), ("a", 2))
        
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd = sc.parallelize(arr)
        val resultRDD: RDD[(String, Int)] = rdd.mapValues(_ + 1)
        resultRDD.collect.foreach(println)
        sc.stop()
    }
}
