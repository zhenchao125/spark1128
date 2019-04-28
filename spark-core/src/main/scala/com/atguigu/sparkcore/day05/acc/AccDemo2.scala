package com.atguigu.sparkcore.day05.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-28 08:48
  */
object AccDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("a", 50, "b", "c", 10, 20))
        val acc = new StringIntAcc
        sc.register(acc)
        rdd1.foreach(x => acc.add(x))
        println(acc.value)
        sc.stop()
        
        
    }
}

/*
累加器
    确实只有累加的功能
    
系统提供了一些常用的累加器, 主要针对值类型

需要自定义累加器
 */
