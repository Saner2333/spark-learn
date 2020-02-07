package com.saner.spark.sql.day02.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object MyAvgDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("MyAvgDemo")
                .getOrCreate()
        spark.udf.register("my_avg", new MyAvg)
        val df: DataFrame = spark.read.json("d:/people.json")
        df.createOrReplaceTempView("people")
        spark.sql("select my_avg(age) from people").show()
    }
}

class MyAvg extends UserDefinedAggregateFunction {
    //数据的数据类型
    override def inputSchema: StructType = StructType(StructField("s", DoubleType) :: Nil)

    //缓存的数据类型
    override def bufferSchema: StructType =
        StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

    //最终聚合后的数据类型
    override def dataType: DataType = DoubleType

    //相同输入是否应该有相同的输出
    override def deterministic: Boolean = true

    //初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0D
        buffer(1) = 0L
    }

    //分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 对传入数据做非空的判断
        if (!input.isNullAt(0)) {
            // 得到传给聚合函数的值
            val value: Double = input.getDouble(0)
            // 更新缓冲区
            buffer(0) = buffer.getDouble(0) + value
            buffer(1) = buffer.getLong(1) + 1
        }
    }

    //分区间聚合
    // 把buffer2的数据跟新到buffer1中
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val value = buffer2.getAs[Double](0)
        buffer1(0) = buffer1.getDouble(0) + value
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //最终结果
    override def evaluate(buffer: Row): Any = {
        println(buffer.getDouble(0), buffer.getLong(1))
        buffer.getDouble(0) / buffer.getLong(1)
    }
}
