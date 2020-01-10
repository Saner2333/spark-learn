package com.saner.spark.project.app

import com.saner.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTop10SessionApp {

    def calcCategorySessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo],
                                 userVisitActionRDD: RDD[UserVisitAction]) = {

        // 1.过滤出来前10的品类id的点击记录
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
            action => cids.contains(action.click_category_id)
        }
        //2.每个品类top10的session(点击次数)
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD
                .map(action => ((action.click_category_id, action.session_id), 1))
        val cidSessionAndCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        val cidSessionAndCountGrouped: RDD[(Long, Iterable[(String, Int)])] = cidSessionAndCount.groupByKey()
        val resultRDD: RDD[(Long, List[(String, Int)])] = cidSessionAndCountGrouped.mapValues {
            it => it.toList.sortBy(-_._2).take(10)
        }
        resultRDD.collect.foreach(println)
    }

    def calcCategorySessionTop10_1(sc: SparkContext, categoryTop10: Array[CategoryCountInfo],
                                   userVisitActionRDD: RDD[UserVisitAction]) = {

        // 1.过滤出来前10的品类id的点击记录
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
            action => cids.contains(action.click_category_id)
        }
        //2.每个品类top10的session(点击次数)
        for (cid <- cids) {
            val cidSidAndCount: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.filter(_.click_category_id == cid).map {
                action => ((action.click_category_id, action.session_id), 1)
            }.reduceByKey(_ + _)
            val result: Map[Long, List[(String, Int)]] = cidSidAndCount.map {
                case ((cid, sid), count) => (cid, (sid, count))
            }.sortBy(_._2._2)
                    .take(10)
                    .groupBy(_._1)
                    .map {
                        case (cid, arr) => (cid, arr.map(_._2).toList)
                    }
            println(result.toList)
        }

    }

}
