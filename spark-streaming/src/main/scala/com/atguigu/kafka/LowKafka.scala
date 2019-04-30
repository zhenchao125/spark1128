package com.atguigu.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-04-30 09:07
  */
object LowKafka {
    // 读取
    def readOffset(kafkaCluster: KafkaCluster, topic: String, group: String): Map[TopicAndPartition, Long] = {
        // 最终返回的所有分区的offset信息
        var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
        // 获取到所有指定topic的分区信息
        val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
        if (topicAndPartitionEither.isRight) { // 有相应的分区信息
            // 获取到topic和分区的信息
            val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionEither.right.get
            // 获取分区和offset信息
            val topicAndPartition2Long: Either[Err, Map[TopicAndPartition, Long]] =
                kafkaCluster.getConsumerOffsets(group, topicAndPartitions)
            
            if (topicAndPartition2Long.isLeft) { // 如果没有分区消费信息, 则表示是第一次消费
                topicAndPartitions.foreach(topicAndPartition => {
                    result += topicAndPartition -> 0L
                })
            } else { // 如果有分区消费信息, 返回最新的分区消费信息
                result = topicAndPartition2Long.right.get
            }
        }
        result
    }
    
    // 保存offset
    def saveOffset(kafkaCluster: KafkaCluster, group: String, topic: String, dstream: InputDStream[String]) = {
        // 每消费一次都是对这个DStream执行一次foreachRDD
        dstream.foreachRDD(rdd => {
            var map = Map[TopicAndPartition, Long]()
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            
            // 每个分区的offset
            val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            ranges.foreach(offsetRange => {
                map += offsetRange.topicAndPartition() -> offsetRange.untilOffset
            })
            kafkaCluster.setConsumerOffsets(group, map)
        })
    }
    
    def main(args: Array[String]): Unit = {
        // kafka 参数
        //kafka参数声明
        val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
        val topic = "spark1128"
        val group = "bigdata"
        
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        var params: Map[String, String] = Map(
            "zookeeper.connect" -> "hadoop201:2181,hadoop202:2181,hadoop203:2181",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
            ConsumerConfig.GROUP_ID_CONFIG -> group
        )
        // 2. 使用SparkConf创建StreamingContext
        val ssc = new StreamingContext(conf, Seconds(5))
        
        val kafkaCluster = new KafkaCluster(params)
        // 启动的时候, 读取上次消费到的offset
        val fromOffset: Map[TopicAndPartition, Long] = readOffset(kafkaCluster, topic, group)
        val dstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            params,
            fromOffset,
            (message: MessageAndMetadata[String, String]) => message.message()
        )
        
        dstream.print(100)
        // 消费之后写入新的offset
        saveOffset(kafkaCluster, group, topic, dstream)
        ssc.start()
        ssc.awaitTermination()
        
    }
}
