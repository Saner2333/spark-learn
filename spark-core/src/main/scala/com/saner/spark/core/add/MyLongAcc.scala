package com.saner.spark.core.add

import org.apache.spark.util.AccumulatorV2

class MyLongAcc extends AccumulatorV2[Long, Long] {
    private var _sum = 0L

    //判断sum是否为0值
    override def isZero: Boolean = _sum == 0

    //复制累加器
    override def copy(): AccumulatorV2[Long, Long] = {
        val acc = new MyLongAcc
        acc._sum = this._sum
        acc
    }

    //重置累加器
    override def reset(): Unit = _sum = 0

    //累加（分区内累加）
    override def add(v: Long): Unit = _sum += v

    //分区间合并
    override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
        case o: MyLongAcc => this._sum += o._sum
        case _ => throw new UnsupportedOperationException("不支持的操作")
    }
    //返回最终结果
    override def value: Long = _sum
}
