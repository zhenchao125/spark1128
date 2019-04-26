package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 16:46
  */
object Test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70))
        
        val rdd2: RDD[Int] = rdd1.map {
            case x => {
                println(x)
                x + 1
            }
        }
        val rdd3 = rdd2.map {
            case x => {
                
                x
            }
        }
        rdd3.collect
        
        rdd3.count
        
        sc.stop()
        
    }
}
