package com.atguigu.sparkcore.day03.PassFunction.a

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 16:29
  */
object SerDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("SerDemo")
            .setMaster("local[*]")
            // 替换默认的序列化机制
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // org.apache.spark.serializer.KryoSerializer
            // 注册需要使用 kryo 序列化的自定义类
            .registerKryoClasses(Array(classOf[Searcher]))
        
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        val searcher = new Searcher("hello")
        val result: RDD[String] = searcher.getMatchedRDD1(rdd)
        result.collect.foreach(println)
    }
}

case class Searcher(val query: String) {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
        s.contains(query)
    }
    
    
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
        rdd.filter(isMatch) //
    }
    
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) = {
        val q = query
        rdd.filter(_.contains(q))
    }
}
