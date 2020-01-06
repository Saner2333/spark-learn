package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Coalesce {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Coalesce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 81, 31, 61, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1,3)
        println(rdd1.getNumPartitions)
        //在减少分区数时不用shuffle
        val rdd2: RDD[Int] = rdd1.coalesce(2,false)
        println(rdd2.getNumPartitions)
        sc.stop()




    }
}
