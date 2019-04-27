package com.atguigu.sparkcore.day04.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 15:17
  */
object ReadDataFromMysql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
    
        val jdbcRdd = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            "select * from user where ? <= id and id <= ?",
            1,
            20,
            2,
            result => result.getInt(1)
        )
        jdbcRdd.collect.foreach(println)
        
        sc.stop()
    }
}
