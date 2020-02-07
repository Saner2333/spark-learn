package com.saner.spark.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


object JdbcWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Write")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = Seq(("lisi", 20), ("zs", 15)).toDF("name", "age")
        
        // 1. 通用的写
        /*df.write.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/rdd")
            .option("user", "root")
            .option("password", "root")
            .option("dbtable", "user2")
            .mode("append")
            .save()*/
        
        // 2. 专用的写
        val props: Properties = new Properties()
        props.put("user", "root")
        props.put("password", "root")
        df.write
            //.mode("append")
            .jdbc("jdbc:mysql://localhost:3306/rdd", "user1", props)
        
        spark.stop()
        
        
    }
}
