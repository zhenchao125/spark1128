package com.atguigu.sparkcore.day03.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-04-26 10:32
  */
object SortByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 =
            sc.parallelize(Array((new User("a", 11), 88), ((new User("a", 10), 95)), ((new User("a", 10), 91)), ((new User("a", 16), 93)), ((new User("a", 20), 95)), ((new User("a", 3), 98))))
        // key必须可以拍下
        val rdd2= rdd1.sortByKey(false)
        println(rdd2.collect.toList)
        sc.stop()
    }
}

case class User(val name:String , val age : Int) extends Ordered[User]{
    override def compare(that: User): Int = {
        this.age - that.age
    }
}
