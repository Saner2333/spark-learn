package com.saner.realtime.app

import com.saner.realtime.bean.AdsInfo
import com.saner.realtime.util.MyKafkaUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait APP {
    def main(args: Array[String]): Unit = {
        // 1. 创建SteamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("APP")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck2")
        // 2. 从kafka读数据  1579078203631,华东,上海,105,2  并封装到样例类中
        val sourceStream: DStream[AdsInfo] = MyKafkaUtil.getKafkaStream(ssc, "ids_log").map(s => {
            val splits: Array[String] = s.split(",")
            AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
        })
        ssc.start()
        ssc.awaitTermination()

    }
}
