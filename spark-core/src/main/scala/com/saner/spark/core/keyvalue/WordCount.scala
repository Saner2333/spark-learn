package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        //1
        val result1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        //2
        val result2: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
        //3
        val result3: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
        //4
//        rdd.groupByKey().foreach(println)
//        rdd.groupBy(_._1).foreach(println)
        val result4: RDD[(String, Int)] = rdd.groupByKey().map {
            case (k, it) => (k, it.sum)
        }
//5
        val result5: RDD[(String, Int)] = rdd.groupBy(_._1).map {
            case (k, it) => (k, it.map(_._2).sum)
        }
        val result6: RDD[(String, Int)] = rdd.groupBy(_._1).mapValues(i=>i.map(_._2).sum)
        result6.collect.foreach(println)
        sc.stop()
    }

}
