package com.saner.spark.core.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JDBCWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List((30, "a", "a", "a"), (50, "b", "b", "a"))
        val rdd1 = sc.parallelize(list1, 2)
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/db"
        val userName = "root"
        val passWd = "root"
        val sql = "insert into tbl_employee values(?,?,?,?)"
        rdd1.foreachPartition(
            it => {
                Class.forName(driver)
                val conn: Connection = DriverManager.getConnection(url, userName, passWd)
                val ps: PreparedStatement = conn.prepareStatement(sql)
                it.foreach {
                    case (i, l, g, e) =>
                        ps.setInt(1, i)
                        ps.setString(2, l)
                        ps.setString(3, g)
                        ps.setString(4, e)
                        ps.addBatch()
                }
                ps.executeBatch()
                ps.close()
                conn.close()

            }
        )
        sc.stop()
    }
}
