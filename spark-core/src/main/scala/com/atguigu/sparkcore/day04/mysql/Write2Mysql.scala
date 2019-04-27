package com.atguigu.sparkcore.day04.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-27 15:31
  */
object Write2Mysql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd: RDD[Int] = sc.parallelize(list1)
    
       
        rdd.foreachPartition(it => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            it.foreach(x => {
                val ps: PreparedStatement = conn.prepareStatement("insert into user values(?)")
                ps.setInt(1, x)
                ps.executeUpdate()
                ps.close()
            })
            conn.close()
        })
    }
}
