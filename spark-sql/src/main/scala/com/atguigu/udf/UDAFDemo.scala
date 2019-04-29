package com.atguigu.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author lzc
  * Date 2019-04-29 09:07
  */
object UDAFDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        // 注册udf函数
        spark.udf.register("my_avg", new MyAvg)
        // df
        val df: DataFrame = spark.read.json("C:\\Users\\lzc\\Desktop\\class_code\\2018_11_28\\06_scala\\spark1128\\spark-sql\\src\\main\\resources\\user.json")
        df.printSchema()
        // 临时表
        df.createTempView("user")
        // 执行sql
        spark.sql("select my_avg(age) from user").show
        Row(10)
        spark.stop()
    }
}
