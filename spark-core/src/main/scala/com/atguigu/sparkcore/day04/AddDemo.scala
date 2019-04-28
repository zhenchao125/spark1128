package com.atguigu.sparkcore.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 16:43
  */
object AddDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20), 2)
        
        var a = 1
        
        rdd1.foreach(x => {
            a += 1
            System.out.println(a);
        })
        println("------------------")
        println("main:" + a)
        sc.stop()
        
        
    }
}
