package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortBy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 81, 31, 61, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        //ascending 升序 ， false ， 降序
        val rdd2: RDD[Int] = rdd1.sortBy(i=>i,ascending = false)
        rdd2.collect().foreach(println)
        sc.stop()
    }

}
