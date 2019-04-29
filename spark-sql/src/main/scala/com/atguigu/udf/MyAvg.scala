package com.atguigu.udf

import java.text.DecimalFormat

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.Nil

/*
avg(sal)

sum/count

 */
class MyAvg extends UserDefinedAggregateFunction {
    // 输入数据的类型  Double
    override def inputSchema: StructType = StructType(StructField("input", DoubleType) :: Nil)
    
    // 缓冲区中的值的类型  Double, Int
    override def bufferSchema: StructType =
        StructType(StructField("sum", DoubleType) :: StructField("count", IntegerType) :: Nil)
    
    // 最终输出的数据的类型  Double
    override def dataType: DataType = StringType
    
    // 输入和输出之间的确定性
    override def deterministic: Boolean = true
    
    // 缓冲区中值的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 存储数据的和
        buffer(0) = 0d
        // 存储数据的个数
        buffer(1) = 0
    }
    
    
    // 分区内的聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 如果传过来的实在不是空,在进行聚集
        if (!input.isNullAt(0)) {
            // 计算新的和
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
            buffer(1) = buffer.getInt(1) + 1
        }
    }
    
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        if (!buffer2.isNullAt(0)) {
            buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
            buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
        }
    }
    
    
    // 最终输出的值
    override def evaluate(buffer: Row): Any = {
        // 2000.22
        new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getInt(1))
    }
}
