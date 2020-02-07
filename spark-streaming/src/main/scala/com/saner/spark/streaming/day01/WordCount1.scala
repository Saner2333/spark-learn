package com.saner.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
        val rddStream: InputDStream[Int] = ssc.queueStream(queue, false)
        rddStream.reduce(_ + _).print(1000)
        ssc.start()
        while (true) {
            val rdd: RDD[Int] = ssc.sparkContext.parallelize(1 to 100)
            queue.enqueue(rdd)
            Thread.sleep(2000)
            print(queue.length)
        }
        ssc.awaitTermination()
    }
}
