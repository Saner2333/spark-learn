package com.saner.spark.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JdbcRead {
    def main(args: Array[String]): Unit = {
        
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Write")
            .getOrCreate()
        // 通用的读
        /*val df = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/rdd")
            .option("user", "root")
            .option("password", "root")
            .option("dbtable", "user1")
            .load()
        df.show()*/
        // 专用的读
        val url = "jdbc:mysql://localhost:3306/rdd"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "root")
        spark.read.jdbc(url, "user1", props).show
        
        spark.stop()
    }
}
