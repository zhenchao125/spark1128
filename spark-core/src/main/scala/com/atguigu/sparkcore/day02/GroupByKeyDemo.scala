package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-24 16:17
  */
object GroupByKeyDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(
            Array("hello", "world", "hello", "hello", "hello", "world"))
        
        val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
        val rdd4: RDD[(String, Int)] = rdd2.aggregateByKey(10)(_ + _ , _ + _)
        /*val rdd3: RDD[(String, Iterable[Int])] = rdd2.groupByKey
        val rdd4: RDD[(String, Int)] = rdd3.map {
            case (key, it) => (key, it.size)
        }*/
        println(rdd4.collect.toList)
        sc.stop()
    }
}
