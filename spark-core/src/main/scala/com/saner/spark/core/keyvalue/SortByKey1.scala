package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKey1 {
    implicit val ord: Ordering[User] = new Ordering[User] {
        override def compare(x: User, y: User): Int = x.age - y.age
    }

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortByKey1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        //        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        //        val rdd2: RDD[(String, Int)] = rdd.sortByKey(ascending = false)
        val rdd: RDD[String] = sc.parallelize("hello world" :: "hello hello" :: "atguigu atguigu" :: Nil)
        val rdd2: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 按照单词的个数降序排列
        //        val rdd2: RDD[(String, Int)] = rdd1.sortBy(_._2, false)
        val rdd3: RDD[(String, Int)] = rdd2
                .map { case (word, count) => (count, word) }
                .sortByKey()
                .map { case (count, word) => (word, count) }
        rdd3.collect.foreach(println)
        sc.stop()
    }

}
