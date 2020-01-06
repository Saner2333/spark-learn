package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Glom {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Glom").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60, 10)
        val rdd1: RDD[Int] = sc.parallelize(list1,3)
        val rdd2: RDD[Array[Int]] = rdd1.glom()
        rdd2.collect().foreach(x => println(x.mkString(",")))
        sc.stop()
    }
}
