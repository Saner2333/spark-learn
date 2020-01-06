package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 81, 31, 61, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x => x % 2)
        rdd2.collect().foreach(println)
        sc.stop()


    }

}
