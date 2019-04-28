package com.atguigu.sparkcore.day04

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 16:24
  */
object ReadFreomHbase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
    
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
    
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        val rows: RDD[String] = hbaseRDD.map {
            case (_, r) => Bytes.toString(r.getRow)
        }
        rows.collect.foreach(println)
    }
}
