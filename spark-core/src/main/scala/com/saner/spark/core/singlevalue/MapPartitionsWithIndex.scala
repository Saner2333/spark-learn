package com.saner.spark.core.chapter01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60)
        //分区切片规则
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        //和mapPartitions(func)类似.
        // 但是会给func多提供一个Int值来表示分区的索引. 所以func的类型是:(Int, Iterator<T>) => Iterator<U>
        val resultRDD = rdd1.mapPartitionsWithIndex((index, it) => it.map(x => (index, x)))
        resultRDD.collect().foreach(println)
        sc.stop()
    }
}
