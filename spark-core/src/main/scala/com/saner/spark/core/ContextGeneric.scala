package com.saner.spark.core

object ContextGeneric {
    //泛型上下文
    def main(args: Array[String]): Unit = {
        implicit val ord = new Ordering[User] {
            override def compare(x: User, y: User): Int = x.age - y.age
        }
        println(max(User(10, "a"), User(20, "b")))
    }

    // 表示必须有一个隐式值   Ordering[T]
    def max[T: Ordering](a: T, b: T): T = {
        val ord: Ordering[T] = implicitly[Ordering[T]]
        if (ord.gteq(a, b)) a else b
    }
}

case class User(age: Int, name: String)
