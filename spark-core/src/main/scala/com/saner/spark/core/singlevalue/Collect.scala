package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Collect {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Collect").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Any] = List(10, 50, 80, 30, 60,"sss","hhh")
        val rdd1: RDD[Any] = sc.parallelize(list1)
        val rdd2: RDD[Int] = rdd1.collect {
            case x: Int if x > 50 => x + 10
        }
        rdd2.collect().foreach(println)
        sc.stop()
    }

}
