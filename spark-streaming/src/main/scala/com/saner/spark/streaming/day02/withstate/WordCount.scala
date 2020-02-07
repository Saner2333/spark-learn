package com.saner.spark.streaming.day02.withstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck1")
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)
        val wordAndOneStream: DStream[(String, Int)] = sourceStream.flatMap(_.split("\\W+")).map((_, 1))
        val wordCountStream: DStream[(String, Int)] = wordAndOneStream.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
            Some(opt.getOrElse(0) + seq.sum)
        })
        wordCountStream.print()
        ssc.start()
        ssc.awaitTermination()
    }

}
