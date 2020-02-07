package com.saner.spark.sql.project

import org.apache.spark.sql.{DataFrame, SparkSession}

//计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。

object SqlApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession.builder()
                .appName("SqlApp")
                .master("local[2]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse/")
                //.config("spark.sql.hive.convertMetastoreParquet", false)
                .getOrCreate()
        // 1. 先把需要的字段查出来  t1
        spark.sql(
            """
              |select
              |     uv.click_product_id,
              |     pi.product_name,
              |     ci.*
              |from user_visit_action uv
              |join product_info pi on uv.click_product_id=pi.product_id
              |join city_info ci on uv.city_id=ci.city_id
              |""".stripMargin).createOrReplaceTempView("t1")
        //注册聚合函数
        spark.udf.register("remark", RemarkUDAF)
        // 2. 按照地区和商品进行分组聚合  t2
        spark.sql(
            """
              |select
              |     area,
              |     product_name,
              |     count(*) ct,
              |     remark(city_name) remark
              |from t1
              |group by area,product_name
              |""".stripMargin).createOrReplaceTempView("t2")
        // 3.每个地区取前3的商品  开窗 rank(row_number, dense_rank)函数   t3
        spark.sql(
            """
              |select
              |     *,
              |     rank() over(partition by area order by ct desc) rk
              |from t2
              |""".stripMargin).createOrReplaceTempView("t3")
        // 4. 取前3
        val df: DataFrame = spark.sql(
            """
              |select
              |     area,
              |     product_name,
              |     ct,
              |     remark
              |from t3
              |where rk <= 3
              |""".stripMargin)
        // 只要碰到一个行动(show, save, insertInto, saveAsTable), 前面所有的sql都执行

        df.coalesce(1) // 如成为果前面有聚合,则df会默认会成为200个分区, 为了减少将来文件个数, 减少分区
                .write.mode("overwrite").saveAsTable("area_city_count")

        spark.close()

    }

}
