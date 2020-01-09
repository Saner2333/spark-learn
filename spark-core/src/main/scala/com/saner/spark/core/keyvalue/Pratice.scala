package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Pratice {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Pratice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val lineRDD: RDD[String] = sc.textFile("d:/agent.log")
        //        时间戳，省份，城市，用户，广告
        //        1516609143869 0 5 66 5
        //        1516609143869 1 3 33 6
        //        1516609143869 6 2 97 21
        //统计出每一个省份广告被点击次数的 TOP3
        //((provin,ad),1)
        val proAdsAndOne = lineRDD.map(
            line => {
                val splits: Array[String] = line.split(" ")
                ((splits(1), splits(4)), 1)
            }
        )
        //(provin,ad),count))
        val proAdsAndCount: RDD[((String, String), Int)] = proAdsAndOne.reduceByKey(_ + _)
        //(provin,(ad,count))
        val proAndAdsCount: RDD[(String, Iterable[(String, Int)])] = proAdsAndCount.map {
            case ((pro, ad), count) => (pro, (ad, count))
        }.groupByKey()
        //排序取前三
        val resultRDD: RDD[(String, List[(String, Int)])] = proAndAdsCount.map {
            case (pro, it) => (pro, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))

        }
        resultRDD.sortBy(_._1).collect.foreach(println)
        sc.stop()

    }

}
