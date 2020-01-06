package com.saner.spark.core.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Map {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Map").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 70, 30, 60)
        val rdd1: RDD[Int] = sc.makeRDD(list1)
        //返回一个新的 RDD, 该 RDD 是由原 RDD 的每个元素经过函数转换后的值而组成. 就是对 RDD 中的数据做转换.
        val resultRDD = rdd1.map(x => x * x)
        println(resultRDD.collect().mkString(","))
        sc.stop()
    }

}
