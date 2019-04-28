package com.atguigu.sparkcore.day05.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-28 11:22
  */
object BroadCastDemo {
    def main(args: Array[String]): Unit = {
        val arr = Array(10, 20)
        
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
        val broadArr: Broadcast[Array[Int]] = sc.broadcast(arr)
        rdd1.foreach(x => {
            val arr: Array[Int] = broadArr.value
        })
        
        sc.stop()
        
    }
}
