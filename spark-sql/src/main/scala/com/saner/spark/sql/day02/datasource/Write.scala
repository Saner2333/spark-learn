package com.saner.spark.sql.day02.datasource

import org.apache.spark.sql.{SaveMode, SparkSession}

object Write {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Write")
                .getOrCreate()
        import spark.implicits._
        val df = Seq(("lisi", 20), ("zs", 15)).toDF("name", "age")
        // 1. 通用的写
        //df.write.format("json").save("d:/user")
        df.write.format("json").mode(SaveMode.Overwrite).save("d:/user")
        // 2. 专用的写
        df.write.mode("overwrite").json("d:/user")
        spark.close()
    }

}
