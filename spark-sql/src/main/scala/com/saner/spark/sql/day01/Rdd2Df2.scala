package com.saner.spark.sql.day01


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.Nil

object Rdd2Df2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Rdd2Df2").master("local[2]").getOrCreate()
        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(List(("lisi", 20), ("zs", 10)))
        val rdd2: RDD[Row] = rdd.map {
            case (name, age) => Row(name, age)
        }
        val schema: StructType = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
        val df: DataFrame = spark.createDataFrame(rdd2, schema)
        df.show(100)
        spark.stop()
    }

}
