package com.atguigu.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-24 16:17
  */
object GroupByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "world", "atguigu", "hello", "are", "go"))
        
        sc.stop()
    }
}
