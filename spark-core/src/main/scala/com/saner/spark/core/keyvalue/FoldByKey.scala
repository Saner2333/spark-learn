package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
        // reduceByKey 分内和分区间聚合逻辑.   foldByKey相比多了一个zero
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
