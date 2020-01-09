package com.saner.spark.core.checkpoint

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CheckPoint {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CheckPoint").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
        val list1 = List(30)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)

        val rdd2 = rdd1.map(x => {
//            println(x , System.currentTimeMillis())
            (x,System.currentTimeMillis())
        })
        val rdd3 = rdd2.filter(x => {
//            println(x + "=>")
            true
        })
        //check通常和cache一起使用，在多起一个job的时候不会出现不同步的现象
        rdd3.checkpoint()
        rdd3.cache()
        println(rdd3.collect().mkString(","))
        println("----------------")
        println(rdd3.collect().mkString(","))
        println("----------------")
        println(rdd3.collect().mkString(","))
        println(rdd3.collect().mkString(","))
        println(rdd3.collect().mkString(","))
        println(rdd3.collect().mkString(","))

        Thread.sleep(1000000000)
        sc.stop()
    }
}
