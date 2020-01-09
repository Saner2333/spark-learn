package com.saner.spark.core.Partitioner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        //sortByKey不是行动算子，但是默认的rangePartitioner分区器源码里有collect行动算子，所以会输出"x...."
        rdd1.map { x =>
            println("x....")
            (x, 1)
        }.sortByKey()

        Thread.sleep(10000000)
        sc.stop()
    }

}
