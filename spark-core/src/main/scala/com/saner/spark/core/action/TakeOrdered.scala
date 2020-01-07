package com.saner.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TakeOrdered {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TakeOrdered").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val arr: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
        println(arr.mkString(","))
        sc.stop()

    }

}
