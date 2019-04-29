package com.atguigu

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author lzc
  * Date 2019-04-28 16:34
  */
object ApiRDDDF {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext
        val rdd: RDD[(String, Int)] = sc.parallelize(Array(("lisi", 10), ("zs", 20), ("zhiling", 40)))
        // 先在RDD中封装成 Row
        val rowRDD: RDD[Row] = rdd.map {
            case (name, age) => Row(name, age)
        }
        // 给每个字段定义类型和字段名字
        //        val types = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
        val types = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
        val df: DataFrame = spark.createDataFrame(rowRDD, types)
        df.show
        spark.stop()
    }
}
