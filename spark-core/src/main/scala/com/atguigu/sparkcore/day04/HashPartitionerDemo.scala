package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 10:48
  */
object HashPartitionerDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")))
        // 把分区号取出来, 检查元素的分区情况
        val rdd2: RDD[(Int, String)] = rdd1.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))
    
        println(rdd2.collect.mkString(","))
    
        // 把 RDD1使用 HashPartitioner重新分区
        val rdd3 = rdd1.partitionBy(new HashPartitioner(5))
        // 检测RDD3的分区情况
        val rdd4: RDD[(Int, String)] = rdd3.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))
        println(rdd4.collect.mkString(","))
    }
}
