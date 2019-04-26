package com.atguigu.sparkcore.day03.PassFunction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019-04-26 15:43
  */
object SerializeDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        val searcher = new Searcher("hello")
        val result: RDD[String] = searcher.getMatchedRDD2(rdd)
        result.collect.foreach(println)
    }
}

// query 为需要查找的子字符串
class Searcher(val query: String) {
    
    def isMatch(s: String) ={
        s.contains("hello")
    }
    
    // 过滤出包含 query字符串的字符串组成的新的 RDD["hello world", "hello atgugiu"]
    def getMatchedRDD1(rdd: RDD[String]) ={
//        rdd.filter(x => x.contains(this.query))  //
    }
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) ={
        val _q = query
        
        rdd.filter(_.contains(_q))
    }
}