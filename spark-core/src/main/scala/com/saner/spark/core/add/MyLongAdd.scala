package com.saner.spark.core.add

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/*
累加器:
    解决了共享变量写的问题!

    1. 定义累计器

    2. 创建累加器对象

    3. 注册

    4. 使用
 */
object MyLongAdd {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyLongAdd").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        val acc = new MapAcc
        // 使用之前要先把累加器注册到SparkContext上
        sc.register(acc, "MyLongAcc")
        val rdd2: RDD[Int] = rdd1.map(x => {
            acc.add(x)
            x
        })
        rdd2.collect
        println(acc.value)
        sc.stop()

    }
}
