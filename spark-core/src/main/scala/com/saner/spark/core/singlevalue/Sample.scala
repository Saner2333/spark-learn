package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Sample {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Sample").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[Int] = List(10, 50, 81, 31, 61, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        // 参数1: 是否放回  参数: 抽取的比例  如果是true [0, 无穷)  false [0,1]
        val rdd2: RDD[Int] = rdd1.sample(false,1)
        rdd2.collect().foreach(println)
        sc.stop()

    }
}
