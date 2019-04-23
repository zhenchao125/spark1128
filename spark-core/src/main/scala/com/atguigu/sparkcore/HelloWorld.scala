package com.atguigu.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("HelloWorld")  //
        val sc = new SparkContext(conf)
        val rdd: RDD[(String, Int)] = sc.textFile(args(0))
            .flatMap(_.split("\\W"))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        val arr: Array[(String, Int)] = rdd.collect
        println(arr.mkString(", "))
        sc.stop()
    }
}
