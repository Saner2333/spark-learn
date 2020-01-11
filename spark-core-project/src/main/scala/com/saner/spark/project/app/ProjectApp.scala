package com.saner.spark.project.app

import com.saner.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.THEAD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[10]")
        val sc: SparkContext = new SparkContext(conf)
        //读取数据
        val sourceRDD: RDD[String] = sc.textFile("d:/user_visit_action.txt")
        //封装数据
        val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
            val splits: Array[String] = line.split("_")
            UserVisitAction(
                splits(0),
                splits(1).toLong,
                splits(2),
                splits(3).toLong,
                splits(4),
                splits(5),
                splits(6).toLong,
                splits(7).toLong,
                splits(8),
                splits(9),
                splits(10),
                splits(11),
                splits(12).toLong)
        })


        //需求1
//        val categoryCountInfoArray: Array[CategoryCountInfo] = CategoryTopApp.statCategoryTop10(sc, userVisitActionRDD)

        //需求2
        //        CategoryTop10SessionApp.calcCategorySessionTop10(sc,categoryCountInfoArray,userVisitActionRDD)
//        CategoryTop10SessionApp.calcCategorySessionTop10_3(sc, categoryCountInfoArray, userVisitActionRDD)
        //需求3
        PageConversionApp.statPageConversionRate(sc, userVisitActionRDD, "1,2,3,4,5,6,7")


    }

}

/*
问题: toList内存溢出

解决方案1:
    使用spark的排序  rdd.sortBy  sortByKey
    问题: 针对整个rdd排序

解决方案2：
    每个cid取10个
    如果RDD只有一个cid 排序取前10

    好处: 不会内存溢出, 任务一定能跑下来
    缺点: 每个cid都会起一个job, 对资源的消耗比较大
解决方案3：


1.过滤出来前10的品类id的点击记录

2. 每个品类的top10session(点击次数)

    ...
=> RDD[(cid, session), 1)] reduceByKey
=> RDD[(cid, session), count)] mpa
=> RDD[(cid, (session, count))] groupByKey
=> RDD[(cid, Iterable[(session, count)])]  map  每个iterator排序, 取前10

 */