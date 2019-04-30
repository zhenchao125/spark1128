package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-04-29 16:37
  */
object HighKafka2 {
    def createSSC(): StreamingContext = {
        // kafka 参数
        //kafka参数声明
        val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
        val topic = "spark1128"
        val group = "bigdata"
        
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        
        // 2. 使用SparkConf创建StreamingContext
        val sctx = new StreamingContext(conf, Seconds(5))
        sctx.checkpoint("./ck2")
        var params: Map[String, String] = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.GROUP_ID_CONFIG -> group
        )
        val kafkaDStream =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sctx, params, Set(topic))
        kafkaDStream.print
        sctx
    }
    def main(args: Array[String]): Unit = {
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck2", createSSC )
        ssc.start()
        ssc.awaitTermination()
    }
}
