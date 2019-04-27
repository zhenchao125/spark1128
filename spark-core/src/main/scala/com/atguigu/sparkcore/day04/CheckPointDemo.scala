package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CheckPointDemo {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        // 设置checkpoint目录
        sc.setCheckpointDir("hdfs://hadoop201:9000/ck1128")
        val rdd1 = sc.parallelize(Array("abc"))
        val rdd2: RDD[String] = rdd1.map(_ + " : " + System.currentTimeMillis()).map(x => {println("xxx"); x})
        rdd2.cache()
        rdd2.checkpoint()
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
        rdd2.collect().foreach(println)
    }
    
}
