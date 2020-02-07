package com.saner.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object MyKafkaUtil {
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "bigdata",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    def getKafkaStream(ssc: StreamingContext, topic: String, otherTopics: String*) = {
        KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](otherTopics :+ topic, kafkaParams)
        ).map(record => record.value())
    }

}
