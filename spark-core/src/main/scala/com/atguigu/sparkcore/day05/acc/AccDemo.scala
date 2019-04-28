package com.atguigu.sparkcore.day05.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-28 08:48
  */
object AccDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 60, 10, 20))
//        val acc: LongAccumulator = sc.longAccumulator
//        val acc = new IntAccumulator
        val acc = new MapAccmulator
        sc.register(acc)  // 使用前先注册
        rdd1.foreach{
            x => acc.add(x)
        }
        
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
