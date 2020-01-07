package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("female", 1), ("male", 5), ("female", 1), ("female", 5), ("male", 2), ("male", 2)))
        // map端聚合 combine 预聚合
        val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
        rdd2.collect.foreach(println)
        sc.stop()
    }

}
