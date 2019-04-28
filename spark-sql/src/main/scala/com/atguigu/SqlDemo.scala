package com.atguigu

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019-04-28 15:40
  */
object SqlDemo {
    def main(args: Array[String]): Unit = {
       
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        // 在rdd -> ds rdd->df df->ds的时候需要隐式转换
        import spark.implicits._
        
        // 2. spark.read...
        val df: DataFrame = spark.read.json("C:\\Users\\lzc\\Desktop\\class_code\\2018_11_28\\06_scala\\spark1128\\spark-sql\\src\\main\\resources\\user.json")
        
//        df.createTempView("user")
        
//        spark.sql("select name from user").show
        /*
        df.rdd.collect.foreach{
            row => println(row.getLong(0))
        }*/
        val ds: Dataset[User] = df.as[User]
        ds.show
        
        spark.stop()
    }
}

case class User(name: String, age: Long)