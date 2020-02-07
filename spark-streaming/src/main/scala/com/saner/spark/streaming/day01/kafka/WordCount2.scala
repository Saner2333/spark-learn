package com.saner.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[2]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        val params: Map[String, String] = Map[String, String](
            "group.id" -> "aaaa",
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        val sourceStream: InputDStream[(String, String)] = KafkaUtils
                .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, Set("aa"))
        sourceStream.flatMap {
            case (_, v) => v.split("\\W+").map((_, 1))
        }.reduceByKey(_ + _).print(1000)
        ssc.start()
        ssc.awaitTermination()

    }
}
