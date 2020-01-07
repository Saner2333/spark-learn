package com.saner.spark.core.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DoubleValue").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        //        val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(0)(_ + _, _ + _)
        //取出每个分区相同key对应值的最大值，然后相加
//        val rdd2: RDD[(String, Int)] = rdd1.aggregateByKey(Int.MinValue)((x, y) => x.max(y), _ + _)

        //同时计算出同一个key每个分区最大值和最小值的和
        val rdd2: RDD[(String, (Int, Int))] = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))(
            {
                case ((max, min), i) => (max.max(i), min.min(i))
            },
            {
                case ((max1, min1), (max2, min2)) => ((max1 + max2), (min1 + min2))
            })

        rdd2.collect().foreach(println)
        sc.stop()
    }
}
