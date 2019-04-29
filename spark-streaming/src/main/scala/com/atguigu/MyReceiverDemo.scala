package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-04-29 15:27
  */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        // 1. SparkConfig
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    
        // 2. 使用SparkConf创建StreamingContext
        val sctx = new StreamingContext(conf, Seconds(5))
        
        val dstream: ReceiverInputDStream[String] = sctx.receiverStream(new MyReceiver("hadoop201", 10000))
        
        val wordCountDStream: DStream[(String, Int)] =
            dstream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
    
        wordCountDStream.print
    
        // 4. 启动 StreamingContext
        sctx.start()
    
        sctx.awaitTermination()
        
    }
}
