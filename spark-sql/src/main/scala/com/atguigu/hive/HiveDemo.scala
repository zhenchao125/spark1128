package com.atguigu.hive

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-04-29 11:10
  */
object HiveDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        
//        spark.sql("show tables").show
//        spark.sql("create table user(id int)")
//        spark.sql("select * from emp").show()
        spark.sql("create database db2")
//        spark.sql("use default")
//        spark.sql("create table tmp(id int, name string)")
        spark.stop()
    }
}
