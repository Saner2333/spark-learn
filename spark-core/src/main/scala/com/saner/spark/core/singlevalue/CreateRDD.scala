package com.saner.spark.core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("createRDD").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val arr1: Array[Int] = Array(10,30,20,40,90)
        val rdd1: RDD[Int] = sc.parallelize(arr1)
        rdd1.collect().foreach(println)
        sc.stop()
    }

}
