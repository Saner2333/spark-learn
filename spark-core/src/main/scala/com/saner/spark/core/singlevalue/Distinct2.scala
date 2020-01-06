package com.saner.spark.core.singlevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Distinct2 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Distinct2").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1: List[User] = List(new User(10,"1"),new User(10,"2"),new User(20,"hhh"),new User(20,"111"))
        val rdd1: RDD[User] = sc.parallelize(list1)
        val rdd2: RDD[User] = rdd1.distinct()
        rdd2.collect().foreach(println)
        sc.stop()
    }

}

case class User(age: Int, name: String) {
    override def hashCode(): Int = age
    /*
    1. 先看hashcode
    2. 再是否为同一个对象
    3. 然后再看equals
     */
    override def equals(obj: Any): Boolean = {
        obj match {
            case User(age, name) => this.age == age
        }
    }
}