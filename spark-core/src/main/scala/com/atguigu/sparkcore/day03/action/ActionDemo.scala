package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize((Array(("a", 1), ("b", 3), ("a", 4))))
        /*val t: (String, Int) = rdd1.reduce {
            case ((r, sum), (ele, num)) => (r + ele, sum + num)
            
        }
        
        println(t)*/
        
        val tuples: Array[(String, Int)] = rdd1.take(2)
        tuples.foreach(println)
        sc.stop()
        
    }
}

/*



 */
