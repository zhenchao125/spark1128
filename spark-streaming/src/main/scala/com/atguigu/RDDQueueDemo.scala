package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-04-29 15:05
  */
object RDDQueueDemo {
    def main(args: Array[String]): Unit = {
        // 1. SparkConfig
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    
        // 2. 使用SparkConf创建StreamingContext
        val sctx = new StreamingContext(conf, Seconds(5))
    
        val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
        val dstream: InputDStream[Int] = sctx.queueStream(queue, false)
        dstream.reduce(_ + _).print
    
        sctx.start
    
        while(true){
            queue += sctx.sparkContext.parallelize(1 to 100)
            Thread.sleep(2000)
        }
    
        sctx.awaitTermination()
    }
}
