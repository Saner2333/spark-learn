package com.saner.spark.core.text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Text {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Text").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        rdd1.saveAsTextFile("d:/hhh")*/

        //在FileInputFormat.java 290 行 Path path = file.getPath(); 打断点，debug查看切片规则
        //大致可以理解为当文件中有文件大小比所有文件大小平均值的1.1倍大，则进行分片
        sc.textFile("path").collect

        sc.stop()
    }
}
