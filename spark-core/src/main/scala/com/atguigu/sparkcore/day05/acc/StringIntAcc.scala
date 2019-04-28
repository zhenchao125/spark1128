package com.atguigu.sparkcore.day05.acc

import org.apache.spark.util.AccumulatorV2

class StringIntAcc extends AccumulatorV2[Any, (List[String], Int)] {
    var list = List[String]()
    var sum = 0
    
    override def isZero: Boolean = list.isEmpty && sum == 0
    
    override def copy(): AccumulatorV2[Any, (List[String], Int)] = {
        val acc = new StringIntAcc
        acc.list = list
        acc.sum = sum
        acc
    }
    
    override def reset(): Unit = {
        list = List[String]()
        sum = 0
    }
    
    override def add(v: Any): Unit = v match {
        case s: String => list :+= s  //list = list :+ s  // list = s::list
        case i: Int => sum += i
        case _ =>
    }
    
    
    override def merge(other: AccumulatorV2[Any, (List[String], Int)]): Unit = other match {
        
        
        case acc: StringIntAcc =>
            list ++= acc.list
            sum += acc.sum
        case _ =>
    }
    
    override def value: (List[String], Int) = (list, sum)
}
