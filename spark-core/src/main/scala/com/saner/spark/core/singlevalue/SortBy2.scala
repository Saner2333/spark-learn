package com.saner.spark.core.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SortBy2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SortBy2").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[String] = List("hello", "abc", "aaa", "abcde")
        val rdd1: RDD[String] = sc.parallelize(list1)
        //字典排序
        //        val rdd2: RDD[String] = rdd1.sortBy(i=>i,ascending = true)
        //        val rdd2: RDD[String] = rdd1.sortBy(i => i.length, ascending = true)
        // 先按照字符串的长度升序, 如果长度相等, 按照字典的降序
        val rdd2 = rdd1
                .sortBy(x => (x.length, x))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.String), ClassTag(classOf[(Int, String)]))
        rdd2.collect().foreach(println)
        sc.stop()
    }

}
