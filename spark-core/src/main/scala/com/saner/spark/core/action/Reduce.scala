package com.saner.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Reduce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        //        val num: Int = rdd1.reduce(_ + _)//240
        //每个分区内用一次, 分区间聚合的时候再用一次   分区数 + 1
        //        val num: Int = rdd1.aggregate(Int.MaxValue)(_ + _, _ + _)//-2147483411
        //        val num: Int = rdd1.aggregate(1)(_ + _, _ + _)//243
        val num: Int = rdd1.fold(1)(_ + _) //243

        println(num)
    }
}
