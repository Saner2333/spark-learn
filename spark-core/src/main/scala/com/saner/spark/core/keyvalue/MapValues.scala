package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapValues {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapValues").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        val rdd3: RDD[(String, Int)] = rdd1.mapValues(_.sum)
        rdd3.collect.foreach(println)
        sc.stop()


    }
}
