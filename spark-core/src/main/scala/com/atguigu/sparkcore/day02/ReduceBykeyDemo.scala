package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-24 15:55
  */
object ReduceBykeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20, 30, 20, 50, 70))
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey(_ + _)
        println(rdd3.collect.toList)
        sc.stop()
        
    }
}
