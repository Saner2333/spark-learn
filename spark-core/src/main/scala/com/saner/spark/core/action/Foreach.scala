package com.saner.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Foreach {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[(Int, Int)] = sc.parallelize(list1).map((_,1))
//        rdd1.foreach(println)
//        rdd1.collect.foreach(println)
        // rdd的数据写入到外部存储比如: mysql
        // 1. rdd1.collect() 先把数据拉到驱动端, 再从驱动端向mysql
        // 2. 分区内的数据, 直接向mysql写
        /* rdd1.foreach(x => {
             // 建立到mysql的连接
             // 写
         })*/
        rdd1.foreachPartition(it => {
            // 建立到mysql的连接
            // 写
        })
        sc.stop()
    }

}
