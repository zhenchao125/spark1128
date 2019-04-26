package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 10:46
  */
object CoGroup {
    def main(args: Array[String]): Unit = {
        var arr1 = Array(("a", 1), ("b", 10), ("b", 20), ("a", 2), ("c", 10))
        var arr2 = Array(("a", 11), ("b", 101), ("b", 200), ("a", 21), ("d", 100))
        
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(arr1)
        val rdd2 = sc.parallelize(arr2)
        
        val resultRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        
        resultRDD.collect.foreach(println)
        sc.stop()
        
        // 使用 cogroup 实现一个内连接
        
    }
}
