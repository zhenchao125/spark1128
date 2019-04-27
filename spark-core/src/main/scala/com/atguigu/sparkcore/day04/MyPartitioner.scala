package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * Author lzc
  * Date 2019-04-27 11:16
  */
class MyPartitioner(num: Int) extends Partitioner {
    override def numPartitions: Int = num
    
    override def getPartition(key: Any): Int = {
//        System.identityHashCode(key) % num.abs
        new Random().nextInt(num)
    }
    
}

object Partitioner {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
        val sc = new SparkContext(conf)
        
        val rdd1 = sc.parallelize(
            Array((10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")), 3)
        val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new MyPartitioner(6))
        val rdd3: RDD[(Int, String)] = rdd2.mapPartitionsWithIndex((index, items) => items.map(x => (index, x._1 + " : " + x._2)))
        println(rdd3.collect.mkString(" "))
        
    }
    
}