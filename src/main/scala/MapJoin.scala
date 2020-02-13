import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val list1 = List(10, 20, 30, 30, 50, 60)
        val list2 = List(20, 10, 30, 40, 20, 60)
        val rdd1: RDD[(Int, Int)] = sc.parallelize(list1, 2).map((_, 1))
        val rdd2: RDD[(Int, String)] = sc.parallelize(list2, 2).map((_, "a"))
        //找一个小的RDD广播出去，然后对另一个RDDmap，在map内完成join的逻辑
        val bdRDD2: Broadcast[Array[(Int, String)]] = sc.broadcast(rdd2.collect)
        val rdd3: RDD[(Int, (Int, String))] = rdd1.flatMap {
            case (k, v) => {
                val arrRDD2: Array[(Int, String)] = bdRDD2.value
                arrRDD2.filter(_._1 == k).map {
                    case (k2, v2) => (k2, (v, v2))
                }
            }
        }
        rdd3.collect.foreach(println)
        sc.stop()
    }

}
