package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 10:46
  */
object MyJoin {
    def main(args: Array[String]): Unit = {
        var arr1 = Array(("a", 1), ("b", 10), ("b", 20), ("a", 2), ("c", 10))
        var arr2 = Array(("a", 11), ("b", 101), ("b", 200), ("a", 21), ("d", 100))
        
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(arr1)
        val rdd2 = sc.parallelize(arr2)
        
        val resultRDD = join(rdd1, rdd2)
        resultRDD.collect.foreach(println)
        sc.stop()
        
    }
    
    // 对两个参数 RDD完成内连接
    def join(rdd1: RDD[(String, Int)], rdd2: RDD[(String, Int)]) = {
        // 1. key相同的先放在一块   [a, (1, 11)] , [a, (1, 21)] ...
        val coRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        coRDD.flatMapValues {
            case (it1, it2) => for (e1 <- it1; e2 <- it2) yield (e1, e2)
        }
        // 2. 在去连接两个RDD
    }
}
