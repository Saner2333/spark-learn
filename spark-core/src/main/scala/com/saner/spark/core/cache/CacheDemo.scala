package com.saner.spark.core.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)

        val rdd2 = rdd1.map(x => {
            println(x + "->")
            x
        })
        val rdd3 = rdd2.filter(x => {
            println(x + "=>")
            true
        })
        // 对rdd3做缓存 内存
//        rdd3.cache()
        rdd3.persist(StorageLevel.MEMORY_ONLY)
        rdd3.collect
        println("----------------")
        rdd3.collect

        Thread.sleep(10000000)
        sc.stop()

    }
}
