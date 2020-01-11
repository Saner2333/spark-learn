package com.saner.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RDD2DS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("CreateDS")
                .getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(Seq(Person("lisi", 20), Person("zs", 21)))
        val ds: Dataset[Person] = rdd.toDS()
        val rdd1: RDD[Person] = ds.rdd
        rdd1.collect.foreach(println)
        spark.close()
    }
}
