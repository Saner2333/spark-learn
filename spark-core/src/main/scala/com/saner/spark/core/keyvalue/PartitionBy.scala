package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(2))
        //将每一个分区的元素合并成一个数组
        val rdd4: RDD[Array[(Int, Int)]] = rdd3.glom()
        rdd4.collect().foreach(x => println("a" + x.mkString(",")))
        sc.stop()
    }
}
