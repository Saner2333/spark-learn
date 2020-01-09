package com.saner.spark.core.TranmitFun

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerDemo {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello saner", "saner", "hahah"), 2)
        val searcher = new Searcher("hello")
        //        val rdd2: RDD[String] = searcher.getMatchedRDD1(rdd)
        //        rdd2.collect.foreach(println)
        val rdd2: RDD[String] = searcher.getMatchedRDD2(rdd)
        rdd2.collect.foreach(println)

    }
}

case class Searcher(val query: String) extends Serializable {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
        s.contains(query)
    }
    //传递函数
    def getMatchedRDD1(rdd: RDD[String]) = {
        // 过滤出包含 query字符串的字符串组成的新的 RDD
        rdd.filter(isMatch)
    }
    //传递变量
    def getMatchedRDD2(rdd: RDD[String]) = {
        // 过滤出包含 query字符串的字符串组成的新的 RDD
        val q = query
        rdd.filter(x => x.contains(q))
    }
}