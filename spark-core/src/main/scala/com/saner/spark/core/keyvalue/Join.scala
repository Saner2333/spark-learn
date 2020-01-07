package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Join {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        val rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
        //内连接
        //        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        //左外连接  补none  Option对象
//        val rdd3: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
        //右外连接
        //        val rdd3: RDD[(Int, (Option[String], String))] = rdd1.rightOuterJoin(rdd2)
        //满外连接
        val rdd3: RDD[(Int, (Option[String], Option[String]))] = rdd1.fullOuterJoin(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
    }
}
