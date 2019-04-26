package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 13:41
  */
object Practice {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        // 1. 获取到每一行数据
        val lines: RDD[String] = sc.textFile(ClassLoader.getSystemResource("agent.log").getPath)
        // 2.  取出省份和广播的id  1
        val proviceAdsOne = lines.map {
            case line => {
                val arr: Array[String] = line.split("\\s+")
                ((arr(1), arr(4)), 1)
            }
        }
        // 3. 按照省份和广告 reduce
        val proviceAdsCount = proviceAdsOne.reduceByKey(_ + _).map {
            case ((pid, aid), count) => (pid, (aid, count))
        }
        // 4.按照省份和广告进行分组
        val groupedRDD: RDD[(String, Iterable[(String, Int)])] = proviceAdsCount.groupByKey
        
        // 5. 对value排序, 取前3
        val resultRDD = groupedRDD.map {
            case (pid, it) => {
                (pid, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
            }
        }
        
        // 5. 打印
        resultRDD.collect.foreach(println)
        sc.stop()
        
    }
}

/*
数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。

1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12

统计出每一个省份广告被点击次数的
=> RDD[(pid, aid)]  map
=> RDD[(pid, aid), 1)), (pid, aid), 1))] reduceByKey
=> RDD[(pid, aid), count)), (pid, aid), count))]  map
=> RDD[(pid, (aid, count)), (pid, (aid, count))] groupByKey
=> RDD[(pid, Iterable(aid, count),...)]   map
RDD[(pid, List(aid, count),...)]
 
 */
