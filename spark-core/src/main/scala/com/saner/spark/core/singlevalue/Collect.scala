package com.saner.spark.core.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Collect {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        rdd1.collect(
            case x: Int
        )
    }

}
