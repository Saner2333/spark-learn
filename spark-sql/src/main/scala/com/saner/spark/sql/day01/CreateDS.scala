package com.saner.spark.sql.day01

import org.apache.spark.sql.{Dataset, SparkSession}

object CreateDS {
        def main(args: Array[String]): Unit = {
            val spark: SparkSession = SparkSession
                    .builder()
                    .master("local[2]")
                    .appName("CreateDS")
                    .getOrCreate()
            import spark.implicits._
            val seq = Seq(Person("lisi", 20), Person("zs", 21))
            val ds: Dataset[Person] = seq.toDS()
//            ds.createOrReplaceTempView("p")
//            spark.sql("select * from p").show()
//            ds.map(person => person.age).show()
//            ds.select("age").show()
            spark.stop()
        }
    }

    case class Person(name: String, age: Int)

/*
df能用的方法, ds都可以用

创建ds:
    先有样例类
    把样例类存入集合中, 调用集合的toDS()(利用了隐式转换)
 */