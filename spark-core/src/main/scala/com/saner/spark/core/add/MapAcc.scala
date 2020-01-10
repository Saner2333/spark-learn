package com.saner.spark.core.add

import org.apache.spark.util.AccumulatorV2

class MapAcc extends AccumulatorV2[Long, Map[String, Double]] {
    // (和, 个数, 平均值)  Map["sum"-> 1000, "count" -> ..., "avg"->...]
    private var map = Map[String, Double]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
        val acc = new MapAcc
        acc.map = this.map
        acc
    }

    override def reset(): Unit = map = Map[String, Double]()

    override def add(v: Long): Unit = {
        map += "sum" -> (map.getOrElse("sum", 0D) + v)
        map += "count" -> (map.getOrElse("count", 0D) + 1)
    }

    override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = other match {
        case o: MapAcc =>
            this.map += "sum" -> (this.map.getOrElse("sum", 0D) + o.map.getOrElse("sum", 0D))
            this.map += "count" -> (this.map.getOrElse("count", 0D) + o.map.getOrElse("count", 0D))
        case _ => throw new UnsupportedOperationException

    }

    override def value: Map[String, Double] = {
        map += "avg" -> (this.map.getOrElse("sum", 0D) / this.map.getOrElse("count", 0D))
        map
    }
}


