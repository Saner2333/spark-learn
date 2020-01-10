package com.saner.spark.core.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Add1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Add1")
        val sc = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        //内置累加器
        val a: LongAccumulator = sc.longAccumulator("first")
        val rdd2: RDD[Int] = rdd1.map(
            x => {
                a.add(1)
                x
            }
        )
        rdd2.collect
        println(a.value)
        sc.stop

    }
}
