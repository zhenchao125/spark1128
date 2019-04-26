package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 09:32
  */
object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 =
            sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)))
        //val resultRDD =  rdd1.combineByKey(v => v  , (c: Int, v: Int) => c + v, (c1:Int , c2:Int) => c1 + c2 )
        // 计算每个 key 对应的 value 的平均值
        /*val resultRDD = rdd1.combineByKey(
            v => (v, 1),
            (sumSize: (Int, Int), v: Int) => (sumSize._1 + v, sumSize._2 + 1),
            (sumSize1: (Int, Int), sumSize2: (Int, Int)) => (sumSize1._1 + sumSize2._1, sumSize1._2 + sumSize2._2)
        )*/
        
        // 计算 每个分区内 key最大值的和  , 最小值的和
        val resultRDD = rdd1.combineByKey(
            v => (v, v),
            (maxMin: (Int, Int), v) => (maxMin._1.max(v), maxMin._2.min(v)),
            (maxMin1: (Int, Int), maxMin2: (Int, Int)) => (maxMin1._1.max(maxMin2._1), maxMin1._2.min(maxMin2._2))
        )
        println(resultRDD.collect.toList)
        sc.stop()
        
    }
}
