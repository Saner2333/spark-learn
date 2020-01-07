package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        val rdd2: RDD[(String, Int)] = rdd.combineByKey(x => x, (_: Int).max(_: Int), (_: Int) + (_: Int))
        rdd2.collect.foreach(println)
        sc.stop()
    }
}
