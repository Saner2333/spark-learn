package com.saner.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Rdd2Df1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("Rdd2Df1")
                .getOrCreate()
        import spark.implicits._
        val rdd: RDD[User] = spark.sparkContext.parallelize(List(User("hhh", 10), User("aaa", 19)))
        val df: DataFrame = rdd.toDF()
        df.show
        spark.stop()
    }
}

case class User(name: String, age: Int)

/*
import spark.implicits._
rdd->df
    1. rdd中存储是元组
        rdd.toDF("c1", "c2")

    2. rdd中存储是样例类
        rdd.toDF

 */