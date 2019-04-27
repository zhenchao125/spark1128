package com.atguigu.sparkcore.day04.readwrite

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 11:25
  */
object TextFileDemo {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("C:\\Users\\lzc\\Desktop\\class_code\\2018_11_28\\06_scala\\spark1128\\input")
        var resultRDD = lines.flatMap(_.split("\\W")).map((_, 1)).reduceByKey(_ + _)
        resultRDD.saveAsTextFile("hdfs://hadoop201:9000/out1128")
        sc.stop()
        
    }
}
