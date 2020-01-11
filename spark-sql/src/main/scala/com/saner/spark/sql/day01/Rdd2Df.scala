package com.saner.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Rdd2Df {
    def main(args: Array[String]): Unit = {
        //入口 SparkSession
        val spark: SparkSession = SparkSession.builder().master("local[2]")
                .appName("Rdd2Df")
                .getOrCreate()
        import spark.implicits._
        // 2. 创建df(把rdd转换df)
        val list1 = List(("lisi", 20), ("zs", 10))
        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(list1)
        val df: DataFrame = rdd.toDF("name", "age")
        // 3. 查询df
        df.show

        // 4. 关闭session
        spark.stop()
    }
}
/*
rdd->df
    1. rdd中存储是元组
        rdd.toDF("c1", "c2")

    2. rdd中存储是样例类

 */