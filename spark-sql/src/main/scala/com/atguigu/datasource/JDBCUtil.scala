package com.atguigu.datasource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-04-29 10:19
  */
object JDBCUtil {
    
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
    
        /*var props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)*/
    
        val jdbcDF = spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "user")
            .load()
        
        jdbcDF.createTempView("user")
        spark.sql("select * from user where id > 20").show
    
        spark.stop()
        
    }
}
