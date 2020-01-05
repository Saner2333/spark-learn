package com.saner.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Hello {

    def main(args: Array[String]): Unit = {
        //创建sparkcontext
        val conf: SparkConf = new SparkConf().setAppName("Hello")
        val sc: SparkContext = new SparkContext(conf)
        //创建rdd
        val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:9000/input")
        //对rdd进行转换操作
        val wordcount: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
        //执行一个行动算子
        val result: Array[(String, Int)] = wordcount.collect()
        result.foreach(println)
        //关闭sparkcontext
        sc.stop()
    }

}
