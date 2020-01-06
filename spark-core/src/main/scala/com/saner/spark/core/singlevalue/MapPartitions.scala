package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitions {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 80, 30, 60)
        val rdd1: RDD[Int] = sc.makeRDD(list1)
        //类似于map(func), 但是是独立在每个分区上运行.所以:Iterator<T> => Iterator<U>
        //假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区。
        val resultRDD = rdd1.mapPartitions(x => x.map(i=>i*i))
        println(resultRDD.collect().mkString(","))
        sc.stop()
    }
}
