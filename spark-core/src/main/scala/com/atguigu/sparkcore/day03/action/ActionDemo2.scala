package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //        val rdd1 = sc.parallelize(Array(1, 2, 3, 4), 3)
        
        //        rdd1.takeOrdered(3)(Ordering.Int.reverse).foreach(println)
        //        println(rdd1.aggregate(1)(_ + _, _ + _)) //
        
        val rdd1 = sc.parallelize(Array("a", "b", "c", "d", "a", "b"), 3)
        //        println(rdd1.aggregate("x")(_ + _, _ + _))
        //        println(rdd1.fold("x")(_ + _))
        // countByKey reduceByKey
        val map: collection.Map[String, Long] = rdd1.map((_, 1)).countByKey()
        println(map)
        sc.stop()
        
    }
}

/*

reduce
    (T, T) => T
fold
    (zero:U)((U, T) => U)
aggregate
    (zero: U)(u, T => u , u , u => u)
 


 */
