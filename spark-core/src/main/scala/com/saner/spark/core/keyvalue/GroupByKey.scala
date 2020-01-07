package com.saner.spark.core.keyvalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1: RDD[(String, Int)] = sc.parallelize(List(("female", 1), ("male", 5),
            ("female", 1), ("female", 5), ("male", 2), ("male", 2)))
        //        val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()

        val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupBy(_._1).map {
            case (k, it) => (k, it.map(_._2))
        }
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
