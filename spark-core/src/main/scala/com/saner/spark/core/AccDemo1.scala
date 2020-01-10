package com.saner.spark.core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

    object AccDemo1 {
        def main(args: Array[String]): Unit = {
            val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
            val sc = new SparkContext(conf)
            val p1 = Person(10)
            // 将来会把对象序列化之后传递到每个节点上
            val rdd1 = sc.parallelize(Array(p1))
            val rdd2: RDD[Person] = rdd1.map(p => {p.age = 100; p})
            println(p1.age)
            rdd2.count()
            // 仍然是 10

        }
    }

    case class Person(var age:Int)


