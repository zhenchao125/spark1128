package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val rdd1 = sc.parallelize(Array("ab", "bc"))
        val rdd2 = rdd1.flatMap(x => {
            println("flatMap...")
            x.split("")
        })
        val rdd3: RDD[(String, Int)] = rdd2.map(x => {
            (x, 1)
        })
        rdd3.cache()
        
        rdd3.collect.foreach(println)
        println("-----------")
        rdd3.collect.foreach(println)
        
    }
    
}
/*
持久化到内存
    cache
持久化到磁盘
 
 */