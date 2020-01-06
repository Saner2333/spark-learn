package com.saner.spark.core.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Filter").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[Int] = rdd1.filter(_ > 50)
        rdd2.collect().foreach(println)
        sc.stop()
    }
}
