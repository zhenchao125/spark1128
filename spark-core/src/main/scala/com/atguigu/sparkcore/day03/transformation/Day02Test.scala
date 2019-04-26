package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day02Test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
//        test4(sc)
        test9(sc)
        
        sc.stop()
        
    }
    
    def test4(sc: SparkContext): Unit ={
        val rdd1: RDD[String] = sc.parallelize(Array("a","b","c","d"),2)
//        val rdd2: RDD[String] = rdd1.mapPartitions(it => Iterator(it.mkString))
        val indexItem: RDD[(Int, String)] = rdd1.mapPartitionsWithIndex((index, it) => {
            it.map((index, _))
        })
        val rdd2: RDD[String] = indexItem.reduceByKey(_ + _).map(_._2)
        
        println(rdd2.collect.toList)
    }
    
    def test9(sc: SparkContext) ={
        val pairRDD: RDD[(String, Int)] = sc.parallelize(Array(("a",1),("a",3),("b",3),("c",5),("b",7)))
        /*val groupedRDD: RDD[(String, Iterable[Int])] = pairRDD.groupByKey
        val resultRDD: RDD[(String, Double)] = groupedRDD.map {
            case (key, it) => {
                (key, it.sum.toDouble / it.size)
            }
        }*/
        
        // (key, (sum, size))
//        val resultRDD = pairRDD.aggregateByKey((0, 0))((t, value) => (t._1 + value, t._2 + 1) , (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
        val resultRDD = pairRDD.aggregateByKey((0, 0))({
            case ((sum, size), value) => (sum + value, size + 1)
        }, {
            case((sum1, size1), (sum2, size2)) => (sum1 + sum2, size1 + size2)
        })
        val result: RDD[(String, Double)] = resultRDD.map {
            case (key, (sum, size)) => (key, sum.toDouble / size)
        }
        println(result.collect().toList)
    }
}

/*
9.创建一个KV类型的RDD，计算每种 K 对应value的平均值。
例如((a,1),(a,3),(b,3),(c,5),(b,7))=>((a,2),(b,5),(c,5))


4.创建一个RDD，使其一个分区的数据转变为一个String。
例如：(Array("a","b","c","d"),2)=>(ab,cd)

mapPartitions

mapPartitionsWithIndex
 */