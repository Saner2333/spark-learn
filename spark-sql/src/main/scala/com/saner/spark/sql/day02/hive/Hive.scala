package com.saner.spark.sql.day02.hive

import org.apache.spark.sql.SparkSession

object Hive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val spark: SparkSession = SparkSession
                .builder().master("local[2]").appName("Hive").enableHiveSupport().getOrCreate()
        spark.sql("use gmall")
        spark.sql("select count(*) from dws_uv_detail_day").show()

        spark.stop()
    }
}
