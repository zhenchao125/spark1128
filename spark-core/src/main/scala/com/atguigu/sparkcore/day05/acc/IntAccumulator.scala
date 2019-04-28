package com.atguigu.sparkcore.day05.acc

import org.apache.spark.util.AccumulatorV2

class IntAccumulator extends AccumulatorV2[Int, Int]{
    var sum = 0
    
    // 判断累加器是不是空
    override def isZero: Boolean = sum == 0
    
    // 如果把累加器copy到executor
    override def copy(): AccumulatorV2[Int, Int] = {
        val acc = new IntAccumulator
        acc.sum = sum
        acc
    }
    
    // 重置
    override def reset(): Unit = {
        sum = 0
    }
    
    // 分区内的累加
    override def add(v: Int): Unit = {
        sum += v
    }
    
    // 分区间的累加
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        other match {
            case o:IntAccumulator => this.sum += o.sum
            case _ =>
        }
    }
    
    // 累加器的最终的值
    override def value: Int = this.sum
    
}
