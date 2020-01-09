package com.saner.spark.core.jdbc

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JDBCRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)

        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/db"
        val userName = "root"
        val passWd = "root"
        val sql = "select last_name from tbl_employee where id > ? and id < ?"
        val rdd = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            sql,
            0,
            10,
            2,
            (result: ResultSet) => {
                result.getString(1)
            }
        )
        rdd.collect.foreach(println)
        sc.stop()


    }

}
