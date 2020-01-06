package com.saner.spark.core.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FlatMap {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        // 得到一个新的rdd, rdd存储的是这个数和他们的平方, 立方
        //        val rdd2: RDD[Int] = rdd1.flatMap(x => Array(x, x * x, x * x * x))
        //        val rdd2: RDD[Int] = rdd1.flatMap(x => if (x > 50) Array(x) else Array[Int]())
        val rdd2: RDD[Char] = rdd1.flatMap(x => x + "")
        rdd2.collect().foreach(println)
        sc.stop()
    }

}
