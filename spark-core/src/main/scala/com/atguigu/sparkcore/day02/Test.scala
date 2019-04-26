package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-24 10:12
  */
object Test {
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 60, 90))
        val pairRDD: RDD[(Int, Int)] = rdd1.map((_, 1))
        println(pairRDD.partitions.length)
        val rdd2: RDD[(Int, Int)] = pairRDD.partitionBy(new HashPartitioner(3))
        println(rdd2.partitions.length)
        var rdd3 = pairRDD.mapPartitionsWithIndex {
            case (index, it) => {
                it.map {
                    case (num, one) => {
                        (index, (num, one))
                    }
                }
            }
        }
        println(rdd3.collect.mkString(" : "))
    }
}
