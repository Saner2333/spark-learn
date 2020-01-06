package com.saner.spark.core.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DoubleValue {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DoubleValue").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 5, 7, 60, 10)
        val list2 = List(3, 5, 7, 6, 1,2)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[Int] = sc.parallelize(list2)
        //并集
        //        val rdd3: RDD[Int] = rdd1.union(rdd2)

        //交集
        //        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        //差集
        //        val rdd3: RDD[Int] = rdd1.subtract(rdd2)
        //拉链
        /*
        zip:
        1. 分区数一样
        2. 每个分区内的元素个数也要一样 (总数一样)
        */
        //        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

        //元素和自己的索引
        //        val rdd3: RDD[(Int, Long)] = rdd1.zipWithIndex()
        //分区一样即可，此处zip是scala中的zip
   /*     val rdd3: RDD[(Int, Int)] = rdd1.zipPartitions(rdd2)((it1, it2) => {
            //            it1.zip(it2)//多余的扔掉
            //多余的取默认值
            it1.zipAll(it2, -1, -2)
        })*/
        //笛卡尔积,不建议使用
        val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

        rdd3.collect.foreach(println)
        sc.stop()
    }
}
