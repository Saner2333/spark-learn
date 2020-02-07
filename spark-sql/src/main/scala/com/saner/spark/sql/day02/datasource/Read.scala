package com.saner.spark.sql.day02.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

object Read {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[2]").appName("Read").getOrCreate()
        // 通用的读方法  默认格式(没有format)是parquet
        val df: DataFrame = spark.read.format("json").load("d:/people.json")
        // 专用的读方法
        //val df: DataFrame = spark.read.json("d:/people.json")

        df.show()
        spark.close()

    }
}
