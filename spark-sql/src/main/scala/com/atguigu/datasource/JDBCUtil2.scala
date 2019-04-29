package com.atguigu.datasource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019-04-29 10:19
  */
object JDBCUtil2 {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        val rdd = spark.sparkContext.parallelize(Array(People(100), People(200)))
        val df = rdd.toDS
        var props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop201:3306/rdd", "user", props)
    
        spark.stop()
        
    }
}
case class People(id: Int)