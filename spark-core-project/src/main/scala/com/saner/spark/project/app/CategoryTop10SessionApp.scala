package com.saner.spark.project.app

import com.saner.spark.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
            val cidSidAndCount: RDD[((Long, String), Int)] =
                filteredUserVisitActionRDD.filter(_.click_category_id == cid).map {
                    action => ((action.click_category_id, action.session_id), 1)
                }.reduceByKey(_ + _)
            val result: Map[Long, List[(String, Int)]] = cidSidAndCount.map {
                case ((cid, sid), count) => (cid, (sid, count))
            }.sortBy(-_._2._2)
                    .take(10)
                    .groupBy(_._1)
                    .map {
                        case (cid, arr) => (cid, arr.map(_._2).toList)
                    }
            println(result.toList)
        }

    }

    def calcCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo],
                                   userVisitActionRDD: RDD[UserVisitAction]) = {

        // 1.过滤出来前10的品类id的点击记录
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
            action => cids.contains(action.click_category_id)
        }
        //2.每个品类top10的session(点击次数)
        val cidSidAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndOne.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        val cidAndSidCountGrouped: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
        val resultRDD: RDD[(Long, List[SessionInfo])] = cidAndSidCountGrouped.mapValues { it => {
            // 创建一个Treeset来进行自动的排序, 每次只取前10个
            var set = new mutable.TreeSet[SessionInfo]()
            it.foreach {
                case (sid, count) =>
                    set += SessionInfo(sid, count)
                    if (set.size > 10) set = set.take(10)
            }
            set.toList
        }
        }
        resultRDD.collect.foreach(println)

    }

    // 对calcCategorySessionTop10_2的改良, 减少一次分区:  在reduceByKey的时候, 保证一个分区内只有一个Category的信息
    def calcCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo],
                                   userVisitActionRDD: RDD[UserVisitAction]) = {

        // 1.过滤出来前10的品类id的点击记录
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
            action => cids.contains(action.click_category_id)
        }
        //2.每个品类top10的session(点击次数)
        val cidSidAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        // 聚合的时候, 让一个分区内只有一个Category, 则后续不再需要分组,提升性能
        val cidAndSidCount: RDD[(Long, (String, Int))] = cidSidAndOne.reduceByKey(new CategoryPartitioner(cids), _ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        val resultRDD: RDD[(Long, SessionInfo)] = cidAndSidCount.mapPartitions { it => {
            // 创建一个Treeset来进行自动的排序, 每次只取前10个
            var set: mutable.TreeSet[SessionInfo] = new mutable.TreeSet[SessionInfo]()
            var categoryId = 0L
            it.foreach {
                case (cid, (sid, count)) =>
                    categoryId = cid
                    set += SessionInfo(sid, count)
                    if (set.size > 10) set = set.take(10)
            }
            set.map((categoryId, _)).toIterator
        }
        }
        resultRDD.collect.foreach(println)

    }
}

class CategoryPartitioner(cids: Array[Long]) extends Partitioner {
    private val map: Map[Long, Int] = cids.zipWithIndex.toMap

    // 分区的个数设置为和品类的id数保持一致, 将来保证一个分区内只有一个品类的数据
    override def numPartitions: Int = cids.length

    // 根据key来返回这个key应用取的那个分区的索引
    override def getPartition(key: Any): Int = {
        key match {
            case (k: Long, _) => map(k)
        }
    }
}