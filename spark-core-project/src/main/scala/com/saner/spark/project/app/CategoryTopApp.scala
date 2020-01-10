package com.saner.spark.project.app

import com.saner.spark.project.acc.CategoryAcc
import com.saner.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CategoryTopApp {
    def statCategoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]): Array[CategoryCountInfo] = {
        val acc = new CategoryAcc
        sc.register(acc, "CategoryAcc")
        userVisitActionRDD.foreach(action => acc.add(action))
        val cidActionAndCountGrouped: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
        val categoryCountInfos: Array[CategoryCountInfo] = cidActionAndCountGrouped.map {
            case (cid, map) => {
                CategoryCountInfo(
                    cid,
                    map.getOrElse((cid, "click"), 0L),
                    map.getOrElse((cid, "order"), 0L),
                    map.getOrElse((cid, "pay"), 0L)
                )
            }
        }.toArray

        categoryCountInfos
                .sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
                .take(10)
    }
}
