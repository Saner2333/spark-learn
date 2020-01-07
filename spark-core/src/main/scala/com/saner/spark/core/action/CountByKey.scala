package com.saner.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd = sc.parallelize(list1).map((_, 1))
        val resultMap: collection.Map[Int, Long] = rdd.countByKey()
        println(resultMap)
        sc.stop()

    }

}
